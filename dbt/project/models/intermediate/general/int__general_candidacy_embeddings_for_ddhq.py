import logging
import time
from typing import List, Optional

import numpy as np
import requests
from google import genai
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from tqdm import tqdm

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

    def _create_embeddings_sequential(
        self,
        texts: List[str],
        model: str = "gemini-embedding-001",
        batch_size: int = 100,
        rate_limit_delay: float = 2.0,
        adaptive_rate_limiting: bool = True,
    ) -> np.ndarray:
        """
        Create embeddings using sequential batch processing with adaptive rate limiting.

        Args:
            texts: List of texts to embed
            model: Embedding model to use
            batch_size: Number of texts to process per batch
            rate_limit_delay: Base delay between batches (seconds)
            adaptive_rate_limiting: Whether to adapt delays based on 429 errors

        Returns:
            numpy array of embeddings
        """
        self.logger.info(
            f"Creating embeddings for {len(texts)} texts using {model} (sequential)"
        )

        # Split texts into batches
        batches = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            batches.append((i // batch_size, batch))

        total_batches = len(batches)
        self.logger.info(f"Processing {total_batches} batches sequentially")

        # Adaptive rate limiting state
        current_delay = rate_limit_delay
        consecutive_429s = 0

        # Process batches sequentially with progress bar
        progress_bar = tqdm(
            total=total_batches, desc="Processing batches", unit="batch"
        )

        all_embeddings = []

        try:
            for batch_num, batch_texts in batches:
                last_exception: Exception | None = None

                # Add rate limiting delay before processing
                if batch_num > 0:  # Don't delay the first batch
                    time.sleep(current_delay)

                for attempt in range(self.max_retries):
                    try:
                        batch_embeddings = []

                        for text in batch_texts:
                            # Use Gemini REST API directly for sync calls
                            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:embedContent"
                            headers = {
                                "Content-Type": "application/json",
                                "x-goog-api-key": self.api_key,
                            }
                            data = {
                                "content": {"parts": [{"text": text}]},
                                "taskType": "RETRIEVAL_DOCUMENT",
                            }

                            response = requests.post(
                                url, json=data, headers=headers, timeout=60.0
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

                        all_embeddings.extend(batch_embeddings)
                        # Track cost for this batch
                        self._track_embedding_cost(batch_texts, model)

                        progress_bar.update(1)
                        self.logger.debug(
                            f"Batch {batch_num + 1} completed successfully ({len(batch_embeddings)} embeddings)"
                        )
                        break  # Success, exit retry loop

                    except requests.exceptions.HTTPError as e:
                        last_exception = e

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
                            time.sleep(retry_delay)
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
                            time.sleep(delay)
                        else:
                            progress_bar.update(1)  # Update progress even on failure
                            raise RuntimeError(
                                f"Failed to create embeddings for batch {batch_num + 1} after {self.max_retries} attempts. Last error: {str(last_exception)}"
                            )

            progress_bar.close()

            self.logger.info(
                f"Successfully created {len(all_embeddings)} embeddings using sequential processing"
            )
            return np.array(all_embeddings)

        except Exception as e:
            progress_bar.close()
            self.logger.error(f"Sequential embedding generation failed: {str(e)}")
            raise RuntimeError(f"Sequential embedding generation failed: {str(e)}")

    def create_embeddings(
        self,
        texts: List[str],
        batch_size: int = 100,
        rate_limit_delay: float = 2.0,
        **kwargs,
    ) -> np.ndarray:
        """
        Create embeddings with sequential processing and rate limiting.

        Args:
            texts: List of texts to embed
            batch_size: Number of texts per batch
            rate_limit_delay: Base delay between batches in seconds
            **kwargs: Additional arguments

        Returns:
            numpy array of embeddings
        """
        # For single texts, always use single embedding
        if len(texts) == 1:
            return self._create_single_embedding(texts[0], **kwargs).reshape(1, -1)

        # For multiple texts, use sequential processing
        return self._create_embeddings_sequential(
            texts,
            batch_size=batch_size,
            rate_limit_delay=rate_limit_delay,
            **kwargs,
        )


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        unique_key=["gp_candidacy_id", "election_type", "election_date"],
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        tags=["intermediate", "general", "candidacy", "embeddings", "ddhq"],
    )

    # get dbt configs
    gemini_api_key = dbt.config.get("gemini_api_key")

    candidacy_clean_for_ddhq: DataFrame = dbt.ref(
        "int__general_candidacy_clean_for_ddhq"
    )

    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_updated_at_row = existing_table.agg({"updated_at": "max"}).collect()[0]
        max_updated_at = max_updated_at_row[0] if max_updated_at_row else None

        if max_updated_at:
            candidacy_clean_for_ddhq = candidacy_clean_for_ddhq.filter(
                candidacy_clean_for_ddhq["updated_at"] >= max_updated_at
            )

    # # downsample in dev testing
    # candidacy_clean_for_ddhq = candidacy_clean_for_ddhq.limit(
    #     1000
    # )  # Adjust limit as needed (100, 71 s), (1000, 380 s)

    candidacy_name_race_texts: List[str] = (
        candidacy_clean_for_ddhq.select("name_race").toPandas()["name_race"].tolist()
    )

    embedding_client = GeminiEmbeddingClient(api_key=gemini_api_key)

    candidacy_name_race_embeddings_original: List[np.ndarray] = (
        embedding_client.create_embeddings(
            texts=candidacy_name_race_texts,
            batch_size=100,
        )
    )

    # Convert to list of arrays if it's a 2D numpy array
    if (
        isinstance(candidacy_name_race_embeddings_original, np.ndarray)
        and len(candidacy_name_race_embeddings_original.shape) == 2
    ):
        candidacy_name_race_embeddings = [
            candidacy_name_race_embeddings_original[i]
            for i in range(candidacy_name_race_embeddings_original.shape[0])
        ]

    # Add row index to preserve order
    candidacy_clean_for_ddhq = candidacy_clean_for_ddhq.withColumn(
        "row_index", monotonically_increasing_id()
    )

    # Create embeddings DataFrame with matching indices
    embeddings_data = [
        (i, embedding.tolist())
        for i, embedding in enumerate(candidacy_name_race_embeddings)
    ]
    embeddings_df = session.createDataFrame(
        embeddings_data, ["row_index", "name_race_embedding"]
    )

    # Join on row index to distribute embeddings across rows
    candidacy_clean_for_ddhq = candidacy_clean_for_ddhq.join(
        embeddings_df, "row_index", "left"
    )
    candidacy_clean_for_ddhq = candidacy_clean_for_ddhq.drop("row_index")

    return candidacy_clean_for_ddhq
