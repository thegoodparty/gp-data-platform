import asyncio
import logging
import time
from typing import List, Optional

import httpx
import numpy as np
from google import genai
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, lit
from tqdm.asyncio import tqdm as atqdm

GEMINI_PRICING = {
    "gemini-2.5-flash": {"input": 0.075, "output": 0.30},
    "gemini-2.5-pro": {"input": 2.50, "output": 7.50},
    "gemini-2.5-flash-lite": {"input": 0.0375, "output": 0.15},
    "gemini-embedding-001": {"input": 0.15, "output": 0.0},
}


"""
Google Gemini Thinking Capabilities by Model as of 2025-09-16:

┌─────────────────┬──────────────────┬─────────────────┬────────────────┐
│ Model           │ Default          │ Budget Range    │ Disable?       │
├─────────────────┼──────────────────┼─────────────────┼────────────────┤
│ 2.5 Pro         │ Dynamic (-1)     │ 128 - 32768     │ No             │
│ 2.5 Flash       │ Dynamic (-1)     │ 0 - 24576       │ Yes (budget=0) │
│ 2.5 Flash Lite  │ No thinking      │ 512 - 24576     │ Yes (budget=0) │
└─────────────────┴──────────────────┴─────────────────┴────────────────┘

Token Tracking Features:
- total_tokens: All tokens used in the request/response
- total_thinking_tokens: Tokens used for internal reasoning
- total_search_tokens: Tokens used for search/grounding operations (tool_use_prompt_token_count)

Usage Examples:
- thinking_budget=0: Disable thinking (Flash models only)
- thinking_budget=-1: Dynamic thinking (model decides)
- thinking_budget=1024: Fixed thinking budget
- include_thoughts=True: Include thought summaries in response
"""


class GeminiEmbeddingClient:
    """
    Specialized Gemini client for embedding generation with advanced rate limiting and parallel processing.

    This client is optimized specifically for Gemini's embedding API and handles:
    - Adaptive rate limiting for 429 errors
    - Parallel batch processing with configurable concurrency
    - Progress tracking with visual progress bars
    - Exponential backoff retry logic
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_retries: int = 5,
        base_delay: float = 1.0,
    ):
        """
        Initialize Gemini embedding client with API key rotation support.

        Args:
            api_key: Gemini API key
            max_retries: Maximum retry attempts per batch
            base_delay: Base delay for exponential backoff (seconds)
        """
        self.api_key = api_key

        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not provided")

        # Initialize logger first
        self.logger = logging.getLogger(__name__)
        self.max_retries = max_retries
        self.base_delay = base_delay

        # Cost tracking
        self.total_embeddings_created = 0
        self.total_input_tokens = 0
        self.total_cost = 0.0

        # Initialize sync Gemini client
        self.genai_client = genai.Client(api_key=self.api_key)

        self.logger.info("GeminiEmbeddingClient initialized")

    def _estimate_token_count(self, text: str) -> int:
        """Estimate token count for text - roughly 4 characters per token."""
        return max(1, len(text) // 4)

    def _track_embedding_cost(
        self, texts: List[str], model: str = "gemini-embedding-001"
    ):
        """Track cost for embedding creation."""
        # total_chars = sum(len(text) for text in texts)  # Not currently used
        estimated_tokens = self._estimate_token_count(" ".join(texts))

        # Calculate cost using pricing
        pricing = GEMINI_PRICING.get(model, GEMINI_PRICING["gemini-embedding-001"])
        cost = (estimated_tokens / 1_000_000) * pricing["input"]

        # Update totals
        self.total_embeddings_created += len(texts)
        self.total_input_tokens += estimated_tokens
        self.total_cost += cost

        self.logger.debug(
            f"Embedding cost: ${cost:.6f} for {len(texts)} texts ({estimated_tokens} tokens)"
        )
        return cost

    def _create_single_embedding(
        self, text: str, model: str = "gemini-embedding-001"
    ) -> np.ndarray:
        """
        Create embedding for a single text with retry logic and API key rotation.

        Args:
            text: Text to embed
            model: Embedding model to use

        Returns:
            numpy array containing the embedding
        """
        for attempt in range(self.max_retries):
            try:
                result = self.genai_client.models.embed_content(
                    model=model, contents=text
                )

                # Track cost
                self._track_embedding_cost([text], model)

                return np.array(result.embeddings[0].values)

            except Exception as e:
                self.logger.warning(
                    f"Single embedding attempt {attempt + 1}/{self.max_retries} failed: {str(e)}"
                )

                if attempt < self.max_retries - 1:
                    delay = self.base_delay * (2**attempt)
                    self.logger.debug(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise RuntimeError(
                        f"Failed to create single embedding after {self.max_retries} attempts: {str(e)}"
                    )

    async def _create_embeddings_parallel(
        self,
        texts: List[str],
        model: str = "gemini-embedding-001",
        batch_size: int = 100,
        max_concurrent_batches: int = 2,
        rate_limit_delay: float = 2.0,
        adaptive_rate_limiting: bool = True,
        stagger_delay: float = 0.1,
    ) -> np.ndarray:
        """
        Create embeddings using parallel batch processing with adaptive rate limiting.

        Args:
            texts: List of texts to embed
            model: Embedding model to use
            batch_size: Number of texts to process per batch
            max_concurrent_batches: Maximum number of concurrent batches
            rate_limit_delay: Base delay between batches (seconds)
            adaptive_rate_limiting: Whether to adapt delays based on 429 errors
            stagger_delay: Delay to stagger batch start times (seconds)

        Returns:
            numpy array of embeddings
        """
        self.logger.info(
            f"Creating embeddings for {len(texts)} texts using {model} (parallel)"
        )

        # Split texts into batches
        batches = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            batches.append((i // batch_size, batch))

        total_batches = len(batches)
        self.logger.info(
            f"Processing {total_batches} batches with max {max_concurrent_batches} concurrent"
        )

        # Adaptive rate limiting state
        current_delay = rate_limit_delay
        consecutive_429s = 0

        # Process batches in parallel with concurrency limit and progress bar
        semaphore = asyncio.Semaphore(max_concurrent_batches)
        progress_bar = atqdm(
            total=total_batches, desc="Processing batches", unit="batch"
        )

        async def process_batch_with_retry(
            batch_num: int, batch_texts: List[str]
        ) -> tuple:
            """Process a single batch with adaptive retry logic"""
            nonlocal current_delay, consecutive_429s

            async with semaphore:
                last_exception = None

                # Add staggered start delay for each batch
                stagger_wait = batch_num * stagger_delay
                if stagger_wait > 0:
                    await asyncio.sleep(stagger_wait)

                # Add rate limiting delay before processing
                if batch_num > 0:  # Don't delay the first batch
                    await asyncio.sleep(current_delay)

                for attempt in range(self.max_retries):
                    try:
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            batch_embeddings = []

                            for text in batch_texts:
                                # Use Gemini REST API directly for async calls
                                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:embedContent"
                                headers = {
                                    "Content-Type": "application/json",
                                    "x-goog-api-key": self.api_key,
                                }
                                data = {
                                    "content": {"parts": [{"text": text}]},
                                    "taskType": "RETRIEVAL_DOCUMENT",
                                }

                                response = await client.post(
                                    url, json=data, headers=headers
                                )
                                response.raise_for_status()

                                result = response.json()
                                embedding = result["embedding"]["values"]
                                batch_embeddings.append(embedding)

                            # Reset consecutive 429s on success
                            if adaptive_rate_limiting and consecutive_429s > 0:
                                consecutive_429s = 0
                                current_delay = max(
                                    rate_limit_delay, current_delay * 0.8
                                )  # Gradually reduce delay
                                self.logger.debug(
                                    f"Reduced rate limit delay to {current_delay:.2f}s after success"
                                )

                            progress_bar.update(1)
                            self.logger.debug(
                                f"Batch {batch_num + 1} completed successfully ({len(batch_embeddings)} embeddings)"
                            )
                            return (
                                batch_num,
                                batch_embeddings,
                                batch_texts,
                            )  # Include texts for cost tracking

                    except httpx.HTTPStatusError as e:
                        last_exception = e

                        # For all other HTTP errors, retry with exponential backoff
                        # Special handling for 429 errors with adaptive rate limiting
                        if e.response.status_code == 429:
                            consecutive_429s += 1

                            if adaptive_rate_limiting:
                                # Exponentially increase delay for 429 errors
                                current_delay = min(
                                    current_delay * 2, 30.0
                                )  # Cap at 30 seconds
                                self.logger.warning(
                                    f"429 Rate limit hit (consecutive: {consecutive_429s}). Increasing delay to {current_delay:.2f}s"
                                )

                        # Exponential backoff for all retryable errors
                        retry_delay = current_delay * (2**attempt)
                        self.logger.warning(
                            f"Batch {batch_num + 1} attempt {attempt + 1}/{self.max_retries} HTTP {e.response.status_code} error. Retrying in {retry_delay:.1f}s"
                        )

                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            progress_bar.update(1)  # Update progress even on failure
                            raise RuntimeError(
                                f"Failed to create embeddings for batch {batch_num + 1} after {self.max_retries} attempts. Last error: {str(last_exception)}"
                            )

                    except Exception as e:
                        last_exception = e
                        self.logger.warning(
                            f"Batch {batch_num + 1} attempt {attempt + 1}/{self.max_retries} failed: {str(e)}"
                        )

                        if attempt < self.max_retries - 1:
                            delay = self.base_delay * (2**attempt)
                            await asyncio.sleep(delay)
                        else:
                            progress_bar.update(1)  # Update progress even on failure
                            raise RuntimeError(
                                f"Failed to create embeddings for batch {batch_num + 1} after {self.max_retries} attempts. Last error: {str(last_exception)}"
                            )
            return ()

        # Execute all batches concurrently
        tasks = [
            process_batch_with_retry(batch_num, batch_texts)
            for batch_num, batch_texts in batches
        ]

        try:
            results = await asyncio.gather(*tasks)
            progress_bar.close()

            # Sort results by batch number and flatten
            results.sort(key=lambda x: x[0])
            all_embeddings = []
            for _, batch_embeddings, batch_texts in results:
                all_embeddings.extend(batch_embeddings)
                # Track cost for this batch
                self._track_embedding_cost(batch_texts, model)

            self.logger.info(
                f"Successfully created {len(all_embeddings)} embeddings using parallel processing"
            )
            return np.array(all_embeddings)

        except Exception as e:
            progress_bar.close()
            self.logger.error(f"Parallel embedding generation failed: {str(e)}")
            raise RuntimeError(f"Parallel embedding generation failed: {str(e)}")

    def create_embeddings(
        self,
        texts: List[str],
        parallel: bool = True,
        batch_size: int = 100,
        max_concurrent_batches: int = 2,
        rate_limit_delay: float = 2.0,
        stagger_delay: float = 0.1,
        **kwargs,
    ) -> np.ndarray:
        """
        Create embeddings with automatic parallel/sync selection and rate limiting.

        Args:
            texts: List of texts to embed
            parallel: Whether to use parallel processing
            batch_size: Number of texts per batch
            max_concurrent_batches: Max concurrent batches (lower = fewer 429s)
            rate_limit_delay: Base delay between batches in seconds
            stagger_delay: Delay to stagger batch start times (seconds)
            **kwargs: Additional arguments

        Returns:
            numpy array of embeddings
        """
        # For single texts, always use single embedding
        if len(texts) == 1:
            return self._create_single_embedding(texts[0], **kwargs).reshape(1, -1)

        # For multiple texts, always use parallel processing (more efficient)
        return asyncio.run(
            self._create_embeddings_parallel(
                texts,
                batch_size=batch_size,
                max_concurrent_batches=max_concurrent_batches,
                rate_limit_delay=rate_limit_delay,
                stagger_delay=stagger_delay,
                **kwargs,
            )
        )


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        unique_key=["candidate_id", "candidate", "race_id"],
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        tags=["intermediate", "ddhq", "election_results", "embeddings"],
    )

    # get dbt configs
    gemini_api_key = dbt.config.get("gemini_api_key")

    ddhq_election_results: DataFrame = dbt.ref("int__ddhq_election_results_clean")
    # candidacy_clean_for_ddhq: DataFrame = dbt.ref("int__general_candidacy_clean_for_ddhq")

    ddhq_name_race_texts: List[str] = (
        ddhq_election_results.select("name_race").toPandas()["name_race"].tolist()
    )
    # candidacy_name_race_texts: List[str] = candidacy_clean_for_ddhq.select("name_race").toPandas()['name_race'].tolist()

    # downsample in dev testing
    ddhq_name_race_texts = ddhq_name_race_texts[:1000]
    # candidacy_name_race_texts = candidacy_name_race_texts[:1000]
    embedding_client = GeminiEmbeddingClient(api_key=gemini_api_key)

    ddhq_name_race_embeddings: List[np.ndarray] = embedding_client.create_embeddings(
        texts=ddhq_name_race_texts,
        parallel=True,
        batch_size=100,
        max_concurrent_batches=50,
    )
    # candidacy_name_race_embeddings: List[np.ndarray] = embedding_client.create_embeddings(
    #     texts=candidacy_name_race_texts,
    #     parallel=True,
    #     batch_size=100,
    #     max_concurrent_batches=50
    # )

    # Convert to list of arrays if it's a 2D numpy array
    if (
        isinstance(ddhq_name_race_embeddings, np.ndarray)
        and len(ddhq_name_race_embeddings.shape) == 2
    ):
        ddhq_name_race_embeddings = [
            ddhq_name_race_embeddings[i]
            for i in range(ddhq_name_race_embeddings.shape[0])
        ]
    # if isinstance(candidacy_name_race_embeddings, np.ndarray) and len(candidacy_name_race_embeddings.shape) == 2:
    # candidacy_name_race_embeddings = [candidacy_name_race_embeddings[i] for i in range(candidacy_name_race_embeddings.shape[0])]

    ddhq_election_results = ddhq_election_results.withColumn(
        "name_race_embedding", array(*[lit(e) for e in ddhq_name_race_embeddings])
    )
    # candidacy_clean_for_ddhq = candidacy_clean_for_ddhq.withColumn("name_race_embedding", array(*[lit(e) for e in candidacy_name_race_embeddings]))

    return ddhq_election_results
