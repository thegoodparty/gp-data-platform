"""The delegate swarm landing's column lists, pinned.

The export, the loader and the staging models are one contract in three places, and
nothing at runtime can tell that they still agree: a column renamed on one side
lands a null or fails a load, and the failure shows up as a mart that is quietly
short rather than as a broken build.

The two tuples below are the pin. They are **literals a human keeps in step**,
because the other sides live in places this test should not reach:

- ``thegoodparty/ops``, ``agent-swarm/host/export-usage-core.ts``: its
  ``SESSION_COST_COLUMNS`` and ``TASK_EDGE_COLUMNS`` are what the exporter writes,
  in this order. No runtime dependency on that repository exists here, and none
  should be added: the pin is the mechanism.
- ``airflow/astro/include/custom_functions/delegate_swarm_export.py``: its
  ``SESSION_COST_LANDING_COLUMNS`` and ``TASK_EDGE_LANDING_COLUMNS`` are what the
  loader lands, in this order. That module's own test pins it from its side.

So this test proves the dbt side against its own copy of the contract, and the
staging models against that copy. A pull request that changes a column on any side
changes the others and says so.

Written as pytest rather than as a dbt test because it reads the model files: a
dbt test runs against the warehouse, and the failure this is here to catch happens
before anything is built.
"""

from __future__ import annotations

import re
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1] / "project"
STAGING = PROJECT / "models" / "staging" / "airflow_source"

# The cost file's columns, in the order the exporter writes them. Must equal
# `SESSION_COST_COLUMNS` in ops `agent-swarm/host/export-usage-core.ts`, and the
# exported half of `SESSION_COST_LANDING_COLUMNS` in the loader.
SESSION_COST_COLUMNS = (
    "id",
    "session_id",
    "task_id",
    "agent_id",
    "agent_name",
    "model",
    "cost_source",
    "kind",
    "total_cost_usd",
    "harness_cost_usd",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "cache_write_5m_tokens",
    "cache_write_1h_tokens",
    "reasoning_output_tokens",
    "thinking_tokens",
    "duration_ms",
    "num_turns",
    "is_error",
    "created_at",
    "exported_at",
)

# The edge file's columns. Must equal `TASK_EDGE_COLUMNS` in the same ops file, and
# the exported half of `TASK_EDGE_LANDING_COLUMNS` in the loader.
TASK_EDGE_COLUMNS = (
    "task_id",
    "parent_task_id",
    "dispatched_by_task_id",
    "status",
    "created_at",
    "exported_at",
)

# What the loader stamps on top: the read time, which is never in the file.
LOADER_STAMPED_COLUMNS = ("loaded_at",)

# The columns the cost staging renames on the way through, so a landing column and
# a staged column can be compared by name.
COST_RENAMES = {"total_cost_usd": "cost_usd"}

TABLES = {
    "session_cost": {
        "model": STAGING / "stg_airflow_source__delegate_swarm_session_cost.sql",
        "landing": SESSION_COST_COLUMNS,
        "renames": COST_RENAMES,
        # Not on the landing: the staging model derives it.
        "derived": (),
    },
    "task_edge": {
        "model": STAGING / "stg_airflow_source__delegate_swarm_task_edge.sql",
        "landing": TASK_EDGE_COLUMNS,
        "renames": {},
        "derived": ("rollup_parent_task_id",),
    },
}


def _renamed_block(path: Path) -> str:
    """The body of the staging model's `renamed` CTE.

    The block is where the landing columns are consumed, so it is where a renamed
    or dropped column shows up. Matching the CTE rather than the whole file keeps
    the dedupe above it out of the comparison.
    """
    text = path.read_text()
    start = text.index("renamed as (")
    # The select list opens on the next line and the CTE closes on the first line
    # that is exactly the closing paren at the outer indent.
    inner = text[start:].split("\n", 1)[1]
    end = inner.index("\n    )\n")
    return inner[:end]


def _selected_entries(path: Path) -> list[str]:
    """One entry per line of the `renamed` CTE's select list, comments dropped."""
    entries = []
    for line in _renamed_block(path).splitlines():
        entry = line.strip().rstrip(",").strip()
        if not entry or entry.startswith("--") or entry in {"select", "from current_rows"}:
            continue
        entries.append(entry)
    return entries


def _output_name(entry: str, path: Path) -> str:
    """The name one select-list entry produces: its alias, else the bare column."""
    match = re.search(r"\bas\s+([a-z][a-z0-9_]*)$", entry, flags=re.IGNORECASE)
    if match is not None:
        return match.group(1)
    if re.fullmatch(r"[a-z][a-z0-9_]*", entry, flags=re.IGNORECASE):
        return entry
    raise AssertionError(f"cannot read a column name out of {entry!r} in {path.name}")


def _input_name(entry: str) -> str | None:
    """The landing column a select-list entry reads, where it reads one plainly."""
    match = re.search(r"\b([a-z][a-z0-9_]*)\s+as\s+[a-z][a-z0-9_]*$", entry, flags=re.IGNORECASE)
    return match.group(1) if match else None


def test_the_cost_landing_columns_are_pinned():
    """A local edit to either copy of the contract fails here."""
    assert SESSION_COST_COLUMNS == (
        "id",
        "session_id",
        "task_id",
        "agent_id",
        "agent_name",
        "model",
        "cost_source",
        "kind",
        "total_cost_usd",
        "harness_cost_usd",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "cache_write_5m_tokens",
        "cache_write_1h_tokens",
        "reasoning_output_tokens",
        "thinking_tokens",
        "duration_ms",
        "num_turns",
        "is_error",
        "created_at",
        "exported_at",
    )


def test_the_edge_landing_columns_are_pinned():
    assert TASK_EDGE_COLUMNS == (
        "task_id",
        "parent_task_id",
        "dispatched_by_task_id",
        "status",
        "created_at",
        "exported_at",
    )


def test_each_staging_model_reads_its_own_source():
    """The loader's table names and the names the sources declare are one list."""
    for table, case in TABLES.items():
        sql = case["model"].read_text()
        assert (
            f'source("airflow_source", "delegate_swarm_{table}")' in sql
        ), f"{case['model'].name} must read the delegate_swarm_{table} source"


def test_staging_never_drops_a_landing_column():
    """A column the export writes and staging never reads is a silent drop, and one
    staging reads that the export does not write is a load failure on the next run."""
    for table, case in TABLES.items():
        read = {
            _input_name(entry) or _output_name(entry, case["model"])
            for entry in _selected_entries(case["model"])
        }
        missing = [column for column in case["landing"] if column not in read]
        assert not missing, f"{table} staging does not read landing column(s) {missing}"


def test_staging_invents_no_landing_column():
    """The reverse direction: every plain column staging selects has to exist in the
    landing table, or the next load fails on a column nothing writes."""
    for table, case in TABLES.items():
        # `loaded_at` counts as real even though the file does not carry it: the
        # loader stamps it, which is why it is in the landing table and not in the
        # pinned file column list.
        landing = set(case["landing"]) | set(LOADER_STAMPED_COLUMNS)
        known = landing | set(case["derived"]) | set(case["renames"].values())
        invented = [
            entry
            for entry in _selected_entries(case["model"])
            if _output_name(entry, case["model"]) not in known
        ]
        assert not invented, f"{table} staging selects column(s) nothing lands: {invented}"


def test_the_cost_rename_is_the_only_one():
    """A second rename would break the guarantee that the staged names are the
    landing names, which is what lets a reader compare the two by eye."""
    for entry in _selected_entries(TABLES["session_cost"]["model"]):
        source = _input_name(entry)
        if source is None:
            continue
        target = _output_name(entry, TABLES["session_cost"]["model"])
        expected = COST_RENAMES.get(source, source)
        assert target == expected, f"{source} must stage as {expected}, not {target}"


def test_loaded_at_is_stamped_by_the_loader_and_never_exported():
    """The loader adds the read time, so it can never be one of the file's columns."""
    for column in LOADER_STAMPED_COLUMNS:
        assert column not in SESSION_COST_COLUMNS
        assert column not in TASK_EDGE_COLUMNS
