"""The dbt macro must emit exactly the legs the metric declares (DATA-2421).

`is_dashboard_view_event` no longer holds its own event list; it reads
`config.meta.anchored_on` off `win_active_candidates_30d`. This guard renders the real
macro source against the real declaration and asserts the emitted predicate carries
every leg, so a declaration the macro cannot actually read fails here rather than
silently narrowing an OKR.

Pure Jinja with stubs for dbt's `execute`, `graph`, `exceptions` and `return`: no dbt,
no warehouse, no network, so it runs anywhere. It is therefore a check on the macro's
logic against the declaration, not on a compiled artifact.
"""

import types
from pathlib import Path

import pytest
import yaml
from jinja2 import Environment
from jinja2.ext import do
from semantic_catalog.anchors import parse_anchors

ROOT = Path(__file__).resolve().parents[2]
MACROS = ROOT / "dbt/project/macros/amplitude_event_taxonomy.sql"
SEM = ROOT / "dbt/project/models/marts/analytics/sem_analytics__users_win.yml"

METRIC = "win_active_candidates_30d"
EVENT_COL = "event_type"
PATH_COL = "event_properties:path::string"


class MacroReturn(Exception):
    """Stands in for dbt's `return`, which unwinds the macro with a value."""

    def __init__(self, value):
        super().__init__(value)
        self.value = value


class CompilerError(Exception):
    """Stands in for dbt's `exceptions.raise_compiler_error`."""


def macro_source(name: str) -> str:
    src = MACROS.read_text()
    start = src.index("{% macro " + name + "(")
    return src[start : src.index("{% macro ", start + 1)]


def jinja_env() -> Environment:
    def _raise(message):
        raise CompilerError(message)

    def _return(value):
        raise MacroReturn(value)

    env = Environment(extensions=[do])
    env.globals["exceptions"] = types.SimpleNamespace(raise_compiler_error=_raise)
    env.globals["return"] = _return
    return env


ENV = jinja_env()
ACCESSOR = ENV.from_string(macro_source("metric_anchored_events"))
PREDICATE = ENV.from_string(
    macro_source("is_dashboard_view_event")
    + f"\n{{{{ is_dashboard_view_event('{EVENT_COL}', '{PATH_COL}') }}}}"
)


def declared_meta() -> dict:
    """The metric's raw `config.meta`, exactly as the macro would see it on the node."""
    doc = yaml.safe_load(SEM.read_text())
    metric = next(m for m in doc["metrics"] if m["name"] == METRIC)
    return metric["config"]["meta"]


def anchored_events(execute: bool, meta: dict):
    graph = types.SimpleNamespace(
        metrics={METRIC: types.SimpleNamespace(name=METRIC, config=types.SimpleNamespace(meta=meta))}
    )
    module = ACCESSOR.make_module({"execute": execute, "graph": graph})
    try:
        vars(module)["metric_anchored_events"](METRIC)
    except MacroReturn as returned:
        return returned.value
    raise AssertionError("metric_anchored_events did not return")


def render(execute: bool, meta: dict) -> str:
    sql = PREDICATE.render(
        execute=execute,
        metric_anchored_events=lambda _name: anchored_events(execute, meta),
    )
    return " ".join(sql.split())


def test_the_accessor_reads_every_declared_leg():
    legs = anchored_events(True, declared_meta())
    assert legs == parse_anchors(yaml.safe_load(SEM.read_text()))[METRIC]


def test_every_declared_leg_appears_in_the_rendered_predicate():
    sql = render(True, declared_meta())
    legs = parse_anchors(yaml.safe_load(SEM.read_text()))[METRIC]
    missing = [leg["event"] for leg in legs if f"'{leg['event']}'" not in sql]
    assert not missing, f"declared but not rendered: {missing}"


def test_each_path_leg_renders_with_its_path_predicate():
    sql = render(True, declared_meta())
    pathed = [leg for leg in parse_anchors(yaml.safe_load(SEM.read_text()))[METRIC] if leg["path"]]
    assert pathed, "the declaration lost its page-path leg"
    for leg in pathed:
        assert f"{EVENT_COL} = '{leg['event']}' and {PATH_COL} = '{leg['path']}'" in sql


def test_parse_time_renders_the_false_fallback():
    assert render(False, declared_meta()) == "(false)"


def test_an_empty_declaration_raises_rather_than_zeroing_the_metric():
    with pytest.raises(CompilerError):
        render(True, {"anchored_on": []})
