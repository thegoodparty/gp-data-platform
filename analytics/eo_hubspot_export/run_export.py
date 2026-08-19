"""Run the compiled dbt analysis and write the export CSV.

Prereq: compile the analysis first so the resolved SQL exists:
    cd dbt/project && dbt compile --select elected_officials_hubspot_export

Then from analytics/:
    export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>
    uv run --with pandas --with databricks-sql-connector --with databricks-sdk \
            python eo_hubspot_export/run_export.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "lib"))
import databricks_conn as dbc

REPO = Path(__file__).resolve().parents[2]
COMPILED = (
    REPO
    / "dbt/project/target/compiled/goodparty_data_catalog/analyses"
    / "elected_officials_hubspot_export.sql"
)
OUT_DIR = Path(os.environ.get("EO_EXPORT_DIR", Path(__file__).parent / "output"))


def main() -> None:
    if not COMPILED.exists():
        raise SystemExit(
            f"Compiled analysis not found at {COMPILED}. Run `dbt compile --select "
            "elected_officials_hubspot_export` from dbt/project first."
        )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / "elected_officials_hubspot_export.csv"

    df = dbc.run_query(COMPILED.read_text())
    df.to_csv(out, index=False)
    print(f"rows={len(df)} cols={len(df.columns)} matched={df['hubspot_contact_id'].notna().sum()}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
