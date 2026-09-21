"""The dbt side must emit exactly the legs the metrics declare (DATA-2421).

`is_dashboard_view_event` no longer holds its own event list; it reads
`config.meta.anchored_on` off `win_active_candidates_30d`. This guard renders the real
macro source against the real declaration and asserts the emitted predicate carries
every leg and nothing else, so a declaration the macro cannot actually read fails here
rather than silently narrowing an OKR.

It also guards the filter one model upstream. `int__amplitude_user_milestones` is one
of the models calling the macro, and it is the one on the Active Candidates path
(`users_win_base` reads it), so an anchor event its WHERE clause drops never reaches
the predicate at all, however correct the predicate is. That filter still names the
activated-user anchors literally, which is a deliberate call: one event name in an
allowlist is far less drift-prone than a union that has already changed three times,
and rebuilding the whole milestone pick from metrics is a bigger change than this
ticket carries. The test below is what makes that call safe.

`int__amplitude_win_activity` and its weekly variant also call the macro, and their
own intake gate (`is_recurrent` plus a hardcoded `Viewed` / `/dashboard` leg) is NOT
derived from the declaration. They feed the separate `users_win_activity` mart, not
Active Candidates, so they are a follow-up rather than part of this guard.

Pure Jinja with stubs for dbt's `execute`, `graph`, `exceptions` and `return`: no dbt,
no warehouse, no network, so it runs anywhere. It is therefore a check on the macro's
logic against the declaration, not on a compiled artifact.
"""

import re
import types
from pathlib import Path

import pytest
import yaml
from jinja2 import Environment
from jinja2.ext import do
from semantic_catalog.anchors import parse_anchors

ROOT = Path(__file__).resolve().parents[2]
MACROS = ROOT / "dbt/project/macros/amplitude_event_taxonomy.sql"
MODELS = ROOT / "dbt/project/models"
SEM = MODELS / "marts/analytics/sem_analytics__users_win.yml"
SERVE_SEM = MODELS / "marts/analytics/sem_analytics__users_serve.yml"
MILESTONES = MODELS / "intermediate/amplitude/int__amplitude_user_milestones.sql"

METRIC = "win_active_candidates_30d"
EVENT_COL = "event_type"
PATH_COL = "event_properties:path::string"

MACRO_CALL = re.compile(r"\{\{\s*is_dashboard_view_event\([^)]*\)\s*\}\}")
SQL_LITERAL = re.compile(r"'([^']*)'")
ACCESSOR_CALL = re.compile(r"metric_anchored_events\(\s*\"([^\"]+)\"\s*\)")


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


def declared_legs() -> dict:
    """Every metric's normalised legs, across both semantic files that declare one."""
    legs: dict = {}
    for path in (SEM, SERVE_SEM):
        legs.update(parse_anchors(yaml.safe_load(path.read_text())))
    return legs


def milestone_filter() -> str:
    """The milestone_events WHERE predicate, with its macro call expanded."""
    src = MILESTONES.read_text()
    block = src[src.index("milestone_events as (") : src.index("dashboard_view_flags as (")]
    rendered = render(True, declared_meta())
    expanded, substitutions = MACRO_CALL.subn(lambda _match: rendered, block)
    assert substitutions == 1, "milestone_events no longer reads the dashboard union from the macro"
    return " ".join(expanded.split())


def test_the_predicate_reads_the_active_candidates_metric():
    """Pointing the macro at a different real metric is the silent way to break this.

    The tests below stub the accessor, so they never see which metric was asked for.
    A non-existent name raises at compile time, but `win_activated_users` is one line
    away in the same file, resolves cleanly, and would render the predicate as a
    single voter-outreach event — zeroing Active Candidates with everything green.
    """
    assert ACCESSOR_CALL.findall(macro_source("is_dashboard_view_event")) == [METRIC]


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


def test_the_rendered_predicate_carries_no_undeclared_event():
    """Narrowing is not the only drift: a re-hardcoded or extra leg widens the metric."""
    sql = render(True, declared_meta())
    legs = parse_anchors(yaml.safe_load(SEM.read_text()))[METRIC]
    declared = {leg["event"] for leg in legs} | {leg["path"] for leg in legs if leg["path"]}
    assert set(SQL_LITERAL.findall(sql)) == declared


def test_parse_time_renders_the_false_fallback():
    assert render(False, declared_meta()) == "(false)"


def test_an_empty_declaration_raises_rather_than_zeroing_the_metric():
    with pytest.raises(CompilerError):
        render(True, {"anchored_on": []})


def test_the_milestone_filter_admits_every_declared_anchor_event():
    """The filter upstream of the predicate must not drop an event the metrics anchor on.

    Covers all three anchored metrics, including the two whose anchors the filter still
    names literally, so re-anchoring any of them fails here.
    """
    admitted = milestone_filter()
    missing = [
        leg["event"]
        for legs in declared_legs().values()
        for leg in legs
        if f"'{leg['event']}'" not in admitted
    ]
    assert not missing, f"declared but dropped by milestone_events: {missing}"


def test_the_milestone_filter_keeps_the_path_leg_condition():
    """'Viewed' is site-wide; admitting it unconditionally would widen the filter hugely."""
    admitted = milestone_filter()
    for leg in (leg for leg in declared_legs()[METRIC] if leg["path"]):
        assert f"{EVENT_COL} = '{leg['event']}' and {PATH_COL} = '{leg['path']}'" in admitted
