"""Tests for the `entities` param validation in `extract_ballotready`.

The validation lives inline in each per-entity task closure (see `extract_ballotready.py`), not
in a standalone helper, so there is nothing importable to call directly. Structural DAG tests
(`test_dag_example.py`) build a `DagBag` but never invoke a task body. Here we pull each task's
`python_callable` off the DAG and call it directly, patching `get_current_context` (and, once a
call is expected to get past validation, `Variable`) via `monkeypatch.setitem` on the function's
own `__globals__` -- the same dict the DAG module's top-level names live in, so this reaches the
real validation code with no production changes.
"""

import logging
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.models import DagBag
from include.custom_functions.ballotready_graphql import ENTITY_SPECS


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


_BR_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "extract_ballotready.py")
with suppress_logging("airflow"):
    _BR_DAG = DagBag(dag_folder=_BR_DAG_FILE).dags.get("extract_ballotready")


def _callable(entity_name: str):
    assert _BR_DAG is not None, f"extract_ballotready failed to load from {_BR_DAG_FILE}"
    return _BR_DAG.get_task(f"extract_{entity_name}").python_callable


def _context(entities: list[str]) -> dict:
    return {"params": {"entities": entities}, "dag_run": SimpleNamespace(run_id="test-run")}


class _ReachedConfig(Exception):
    """Raised by the fake `Variable.get` so a test can prove a call got past the entities
    checks (raise-on-unknown, then skip-on-not-requested) without standing up real Databricks
    config, S3, or GraphQL dependencies.
    """


class _FakeVariable:
    @staticmethod
    def get(name):
        raise _ReachedConfig(name)


def _run_past_validation(entity_name: str, entities: list[str], monkeypatch):
    """Call `entity_name`'s task with `entities` requested, patched so a call that clears both
    the unknown-name check and the skip check raises `_ReachedConfig` instead of touching real
    infra.
    """
    func = _callable(entity_name)
    monkeypatch.setitem(func.__globals__, "get_current_context", lambda: _context(entities))
    monkeypatch.setitem(func.__globals__, "Variable", _FakeVariable)
    return func()


def test_unknown_entity_name_raises_and_names_it(monkeypatch):
    func = _callable("party")
    monkeypatch.setitem(func.__globals__, "get_current_context", lambda: _context(["candidacies"]))

    with pytest.raises(ValueError, match=r"candidacies"):
        func()


def test_multiple_unknown_names_are_all_named_in_order(monkeypatch):
    func = _callable("party")
    monkeypatch.setitem(
        func.__globals__, "get_current_context", lambda: _context(["zzz_unknown", "aaa_unknown"])
    )

    with pytest.raises(ValueError) as exc_info:
        func()
    assert "['aaa_unknown', 'zzz_unknown']" in str(exc_info.value)


def test_empty_entities_runs_all_nine(monkeypatch):
    """An empty list must run every entity: this is what a normal scheduled run passes, so a
    regression here means every scheduled run silently does nothing.
    """
    for entity_name in ENTITY_SPECS:
        with pytest.raises(_ReachedConfig):
            _run_past_validation(entity_name, [], monkeypatch)


def test_valid_subset_is_not_treated_as_unknown(monkeypatch):
    with pytest.raises(_ReachedConfig):
        _run_past_validation("issue", ["issue"], monkeypatch)


def test_raise_precedes_the_per_entity_skip_check(monkeypatch):
    """`party` is not in the requested list, which would normally make it a no-op skip -- but an
    unrelated unknown name in that same list must still fail the run loudly instead.
    """
    func = _callable("party")
    monkeypatch.setitem(func.__globals__, "get_current_context", lambda: _context(["bogus"]))

    with pytest.raises(ValueError, match=r"bogus"):
        func()
