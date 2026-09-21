"""The compiled predicate must contain every declared leg (DATA-2421).

Guards against a stale partial parse silently compiling an old leg list.
Requires `dbt compile --select int__amplitude_user_milestones` to have run.
"""

from pathlib import Path

import pytest
import yaml
from semantic_catalog.anchors import parse_anchors

ROOT = Path(__file__).resolve().parents[2]
SEM = ROOT / "dbt/project/models/marts/analytics/sem_analytics__users_win.yml"
COMPILED = ROOT / (
    "dbt/project/target/compiled/goodparty_data_catalog/models/intermediate/"
    "amplitude/int__amplitude_user_milestones.sql"
)


@pytest.mark.skipif(not COMPILED.exists(), reason="run dbt compile first")
def test_every_declared_leg_appears_in_the_compiled_sql():
    legs = parse_anchors(yaml.safe_load(SEM.read_text()))["win_active_candidates_30d"]
    sql = COMPILED.read_text()
    missing = [leg["event"] for leg in legs if f"'{leg['event']}'" not in sql]
    assert not missing, f"declared but not compiled: {missing}"


@pytest.mark.skipif(not COMPILED.exists(), reason="run dbt compile first")
def test_the_path_leg_compiles_with_its_path_predicate():
    sql = COMPILED.read_text()
    assert "'/dashboard'" in sql
