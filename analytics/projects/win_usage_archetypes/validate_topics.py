"""Second-method check on the chat topic classifier: ai_classify vs an instructed LLM on a sample.

Only label pairs leave the warehouse, never message text.
Run from analytics/: uv run --with pyarrow python projects/win_usage_archetypes/validate_topics.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
sys.path.insert(0, str(HERE))

import build_working_set as b  # noqa: E402
import databricks_conn as dbc  # noqa: E402

MODEL = "databricks-claude-opus-5-5"


def main() -> None:
    run_ts = pd.read_parquet(b.OUT / "working_set.parquet", columns=["run_ts"])["run_ts"].iloc[0]
    labels = ", ".join(f"'{t}'" for t in b.TOPICS)
    prompt = (
        "Classify this message a political candidate sent to an AI campaign assistant into exactly one "
        "category. Answer with the category text only. Categories: " + "; ".join(b.TOPICS) + ". Use "
        "'how to use the app' only when they ask how the software itself works; use 'complaint or "
        "frustration' only when the main point is a complaint. Message: "
    )
    sql = f"""
with {b.population_sql(run_ts)},
msgs as (
  select m.content
  from pop p
  join goodparty_data_catalog.airbyte_source.gp_api_db_chat_conversation cc
    on cc.owner_user_id = p.user_id and cc.scope = 'campaign_assistant'
  join goodparty_data_catalog.airbyte_source.gp_api_db_chat_message m
    on m.conversation_id = cc.id and m.role = 'user' and m.created_at >= p.su and m.created_at < p.we
  where length(trim(m.content)) > 0
  order by hash(m.id) limit 300
)
select ai_classify(content, array({labels})) as a,
  lower(trim(ai_query('{MODEL}', concat('{prompt.replace("'", "''")}', content)))) as b
from msgs
"""
    d = dbc.run_query(sql)
    d["b"] = d["b"].str.strip(" .'\"")
    d["agree"] = d["a"] == d["b"]
    print(f"n={len(d)} overall agreement={d['agree'].mean():.0%}")
    print(pd.crosstab(d["a"], d["b"]).to_string())
    d[["a", "b"]].to_csv(b.OUT / "topic_validation_pairs.csv", index=False)


if __name__ == "__main__":
    main()
