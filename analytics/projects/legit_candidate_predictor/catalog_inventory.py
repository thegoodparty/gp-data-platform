"""DATA-2239 Area 2 step 0: inventory the warehouse so feature sourcing is exhaustive.

Caches the full table + column list for the feature-relevant schemas to
`datasets/catalog_columns.parquet`, then prints a grouped summary: table families by
source system, and for each table the join keys it exposes (the identifier columns that
could connect it to the predictor's working set).

Rerun after any warehouse change. Read-only; touches information_schema only.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from databricks_conn import run_query

HERE = Path(__file__).resolve().parent
CACHE = HERE / "datasets" / "catalog_columns.parquet"

SCHEMAS = (
    "airbyte_source",
    "dbt",
    "dbt_stripe",
    "mart_analytics",
    "mart_civics",
    "mart_mban2026",
    "model_predictions",
    "models_mban",
    "exports_zapier",
)

SQL = """
select c.table_schema, c.table_name, t.table_type, c.column_name, c.data_type
from goodparty_data_catalog.information_schema.columns c
join goodparty_data_catalog.information_schema.tables t
  on t.table_schema = c.table_schema and t.table_name = c.table_name
where c.table_schema in ({schemas})
"""

# Identifier columns that can bridge a table to the predictor working set.
JOIN_KEY_PATTERNS = (
    "user_id",
    "campaign_id",
    "candidacy_id",
    "candidate_id",
    "person_id",
    "position_id",
    "race_id",
    "election_id",
    "email",
    "phone",
    "device_id",
    "amplitude_id",
    "anonymous_id",
    "lalvoterid",
    "voterid",
    "l2_",
    "district",
    "zip",
    "state",
    "slug",
    "first_name",
    "last_name",
    "full_name",
)


def fetch(force: bool = False) -> pd.DataFrame:
    if CACHE.exists() and not force:
        return pd.read_parquet(CACHE)
    schemas = ", ".join(f"'{s}'" for s in SCHEMAS)
    df = run_query(SQL.format(schemas=schemas))
    CACHE.parent.mkdir(exist_ok=True)
    df.to_parquet(CACHE, index=False)
    return df


def family(schema: str, table: str) -> str:
    """Bucket a table into a source-system family from its name."""
    t = table.lower()
    for token, label in (
        ("ballotready", "ballotready"),
        ("techspeed", "techspeed"),
        ("ddhq", "ddhq"),
        ("decision_desk", "ddhq"),
        # haystaq before l2: the haystaq tables are all named l2_*_haystaq_*, so an
        # l2-first rule hides the whole family inside the L2 bucket.
        ("haystaq", "haystaq"),
        ("l2_", "l2"),
        ("amplitude", "amplitude"),
        ("segment", "segment"),
        ("gp_api", "gp_api_product"),
        ("gp_webapp", "gp_api_product"),
        ("hubspot", "hubspot"),
        ("stripe", "stripe"),
        ("google", "google"),
        ("mailgun", "email_delivery"),
        ("sendgrid", "email_delivery"),
        ("twilio", "sms"),
        ("ecanvasser", "ecanvasser"),
        ("census", "census"),
        ("acs", "census"),
        ("openai", "llm_enrichment"),
        ("llm", "llm_enrichment"),
        ("mban", "mban"),
        ("civics", "civics_entity_resolution"),
        ("icp", "icp"),
    ):
        if token in t:
            return label
    return f"other:{schema}"


if __name__ == "__main__":
    force = "--force" in sys.argv
    df = fetch(force)
    print(f"columns rows: {len(df):,}  tables: {df.groupby(['table_schema', 'table_name']).ngroups:,}\n")

    tables = (
        df.groupby(["table_schema", "table_name", "table_type"])
        .agg(n_cols=("column_name", "size"), cols=("column_name", lambda s: sorted(s)))
        .reset_index()
    )
    tables["family"] = [family(s, t) for s, t in zip(tables.table_schema, tables.table_name, strict=True)]
    tables["join_keys"] = tables.cols.apply(
        lambda cs: sorted({c for c in cs if any(p in c.lower() for p in JOIN_KEY_PATTERNS)})
    )

    print("=== tables by family x schema ===")
    print(
        tables.pivot_table(
            index="family", columns="table_schema", values="n_cols", aggfunc="size", fill_value=0
        ).to_string()
    )

    out = HERE / "datasets" / "catalog_tables.parquet"
    tables.to_parquet(out, index=False)
    print(f"\nsaved -> {out}")
