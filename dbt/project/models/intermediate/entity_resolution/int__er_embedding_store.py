import subprocess
import sys
import time

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType

# Install google-generativeai at runtime if not available
try:
    import google.generativeai  # noqa: F401
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "google-generativeai"]
    )

EMBEDDING_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("embedding_text", StringType(), False),
        StructField("embedding", ArrayType(FloatType()), True),
        StructField("embedding_model", StringType(), False),
    ]
)

EMBEDDING_MODEL = "gemini/text-embedding-004"
BATCH_SIZE = 100
RATE_LIMIT_DELAY = 0.5  # seconds between batches


def build_embedding_text(row: pd.Series) -> str:
    """Construct the text string to embed for a candidate record."""
    parts = [
        f"Candidate: {row.get('first_name_raw', '')} {row.get('last_name_raw', '')}",
        f"Office: {row.get('candidate_office_clean', '')} ({row.get('office_type_clean', '')})",
        f"Location: {row.get('city_clean', '')}, {row.get('state_abbr', '')}, District {row.get('district_clean', '')}",
        f"Election: {row.get('election_date', '')}",
        f"Party: {row.get('party_clean', '')}",
        f"Level: {row.get('office_level_clean', '')}",
    ]
    return " | ".join(parts)


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="record_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "entity_resolution"],
    )

    features: DataFrame = dbt.ref("int__er_match_features_union")

    # Determine which records need embeddings
    if dbt.is_incremental:
        existing = session.table(f"{dbt.this}")
        existing_ids = existing.select("record_id")
        new_records = features.join(existing_ids, on="record_id", how="left_anti")
    else:
        new_records = features

    new_count = new_records.count()
    if new_count == 0:
        # Return existing table as-is for incremental, or empty for full refresh
        if dbt.is_incremental:
            return session.table(f"{dbt.this}")
        return session.createDataFrame([], schema=EMBEDDING_SCHEMA).withColumn(
            "embedded_at", current_timestamp()
        )

    # Collect new records to pandas for embedding generation
    records_pdf = new_records.select(
        "record_id",
        "source_system",
        "first_name_raw",
        "last_name_raw",
        "candidate_office_clean",
        "office_type_clean",
        "city_clean",
        "state_abbr",
        "district_clean",
        "election_date",
        "party_clean",
        "office_level_clean",
    ).toPandas()

    # Build embedding text
    records_pdf["embedding_text"] = records_pdf.apply(build_embedding_text, axis=1)
    records_pdf["embedding_model"] = EMBEDDING_MODEL

    # Get Gemini API key from Databricks secrets
    dbt_env = dbt.config.get("dbt_environment")
    api_key = dbutils.secrets.get(  # type: ignore[name-defined]
        scope=f"dbt-secrets-{dbt_env}", key="gemini-api-key"
    )

    # Generate embeddings in batches
    import google.generativeai as genai

    genai.configure(api_key=api_key)

    all_embeddings = []
    texts = records_pdf["embedding_text"].tolist()

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]
        try:
            result = genai.embed_content(
                model="models/text-embedding-004",
                content=batch,
            )
            all_embeddings.extend(result["embedding"])
        except Exception as e:
            # On failure, append None for each item in the batch
            all_embeddings.extend([None] * len(batch))
            print(f"Embedding batch {i // BATCH_SIZE} failed: {e}")

        if i + BATCH_SIZE < len(texts):
            time.sleep(RATE_LIMIT_DELAY)

    records_pdf["embedding"] = all_embeddings

    # Create Spark DataFrame with results
    result_sdf = session.createDataFrame(
        records_pdf[
            [
                "record_id",
                "source_system",
                "embedding_text",
                "embedding",
                "embedding_model",
            ]
        ],
        schema=EMBEDDING_SCHEMA,
    )
    result_sdf = result_sdf.withColumn("embedded_at", current_timestamp())

    return result_sdf
