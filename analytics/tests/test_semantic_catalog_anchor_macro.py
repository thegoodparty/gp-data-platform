"""The dbt side must emit exactly the legs the metrics declare (DATA-2421).

`is_dashboard_view_event` no longer holds its own event list; it reads
`config.meta.anchored_on` off `win_active_candidates_30d`. This guard renders the real
macro source against the real declaration and asserts the emitted predicate carries
every leg and nothing else, so a declaration the macro cannot actually read fails here
rather than silently narrowing an OKR.

`is_outreach_activation_event` is the same construction for `win_activated_users`,
and carries one thing the dashboard macro does not: a leg can exclude an event
property. One event name covers three moments there, and only the declaration knows
which of them the metric counts, so the guards below check the exclusion renders and
that the macro refuses a qualifier it cannot compile.

It also guards the filter one model upstream. `int__amplitude_user_milestones` is one
of the models calling both macros, and it is the one on the Active Candidates path
(`users_win_base` reads it), so an anchor event its WHERE clause drops never reaches
the predicate at all, however correct the predicate is. Neither anchor is named
literally in that filter any more: both arrive through their macro, which is what the
milestone guards below expand.

`int__amplitude_win_activity` and its weekly variant also call both macros. Their
intake gate is `is_recurrent` plus a hardcoded `Viewed` / `/dashboard` leg, and
`is_recurrent` is now itself derived from the same two declarations, so a leg added to
a metric reaches those rollups without a second edit.

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
ACTIVATION_METRIC = "win_activated_users"
EVENT_COL = "event_type"
PATH_COL = "event_properties:path::string"
METHOD_COL = "event_properties:method::string"

MACRO_CALL = re.compile(r"\{\{\s*is_dashboard_view_event\([^)]*\)\s*\}\}")
ACTIVATION_MACRO_CALL = re.compile(r"\{\{\s*is_outreach_activation_event\([^)]*\)\s*\}\}")
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


ACTIVATION_PREDICATE = ENV.from_string(
    macro_source("is_outreach_activation_event")
    + f"\n{{{{ is_outreach_activation_event('{EVENT_COL}', '{METHOD_COL}') }}}}"
)


def declared_meta(metric: str = METRIC) -> dict:
    """The metric's raw `config.meta`, exactly as the macro would see it on the node."""
    doc = yaml.safe_load(SEM.read_text())
    found = next(m for m in doc["metrics"] if m["name"] == metric)
    return found["config"]["meta"]


def anchored_events(execute: bool, meta: dict, metric: str = METRIC):
    graph = types.SimpleNamespace(
        metrics={metric: types.SimpleNamespace(name=metric, config=types.SimpleNamespace(meta=meta))}
    )
    module = ACCESSOR.make_module({"execute": execute, "graph": graph})
    try:
        vars(module)["metric_anchored_events"](metric)
    except MacroReturn as returned:
        return returned.value
    raise AssertionError("metric_anchored_events did not return")


def render(execute: bool, meta: dict) -> str:
    sql = PREDICATE.render(
        execute=execute,
        metric_anchored_events=lambda _name: anchored_events(execute, meta),
    )
    return " ".join(sql.split())


def render_activation(execute: bool, meta: dict) -> str:
    sql = ACTIVATION_PREDICATE.render(
        execute=execute,
        metric_anchored_events=lambda _name: anchored_events(execute, meta, ACTIVATION_METRIC),
    )
    return " ".join(sql.split())


def declared_legs() -> dict:
    """Every metric's normalised legs, across both semantic files that declare one."""
    legs: dict = {}
    for path in (SEM, SERVE_SEM):
        legs.update(parse_anchors(yaml.safe_load(path.read_text())))
    return legs


def milestone_filter() -> str:
    """The milestone_events WHERE predicate, with both macro calls expanded."""
    src = MILESTONES.read_text()
    block = src[src.index("milestone_events as (") : src.index("dashboard_view_flags as (")]
    dashboard = render(True, declared_meta())
    expanded, substitutions = MACRO_CALL.subn(lambda _match: dashboard, block)
    assert substitutions == 1, "milestone_events no longer reads the dashboard union from the macro"
    activation = render_activation(True, declared_meta(ACTIVATION_METRIC))
    expanded, substitutions = ACTIVATION_MACRO_CALL.subn(lambda _match: activation, expanded)
    assert substitutions == 1, "milestone_events no longer reads the outreach terminals from the macro"
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


def test_the_activation_predicate_reads_the_activated_users_metric():
    """The mirror of the dashboard guard above, and the same one-line hazard.

    `win_active_candidates_30d` is a few lines away in the same file and resolves
    cleanly, so pointing this macro at it would render the dashboard union as an
    outreach predicate and read activation off page views.
    """
    assert ACCESSOR_CALL.findall(macro_source("is_outreach_activation_event")) == [ACTIVATION_METRIC]


def test_every_declared_activation_leg_appears_in_the_rendered_predicate():
    sql = render_activation(True, declared_meta(ACTIVATION_METRIC))
    legs = parse_anchors(yaml.safe_load(SEM.read_text()))[ACTIVATION_METRIC]
    missing = [leg["event"] for leg in legs if f"'{leg['event']}'" not in sql]
    assert not missing, f"declared but not rendered: {missing}"


def test_the_rendered_activation_predicate_carries_no_undeclared_event():
    """A re-hardcoded or extra leg widens an OKR as surely as a dropped one narrows it."""
    sql = render_activation(True, declared_meta(ACTIVATION_METRIC))
    legs = parse_anchors(yaml.safe_load(SEM.read_text()))[ACTIVATION_METRIC]
    declared = {leg["event"] for leg in legs}
    declared |= {value for leg in legs for value in leg["excluding"].values()}
    # The empty string is the coalesce sentinel that keeps a null method in, not an
    # event name. Pinned rather than filtered out, so losing it fails this test too.
    assert set(SQL_LITERAL.findall(sql)) == declared | {""}


def test_the_excluded_method_renders_as_a_negative_condition():
    """Self-report must be excluded by the emitted SQL, not just by the declaration.

    A declaration carrying an exclusion the predicate ignores is the worst of both:
    the metric reads correctly documented and counts the thing it says it excludes.
    """
    sql = render_activation(True, declared_meta(ACTIVATION_METRIC))
    assert (
        f"{EVENT_COL} = 'Voter Outreach - Campaign Completed' "
        f"and coalesce({METHOD_COL}, '') not in ( 'manual' )"
    ) in sql


def test_a_null_method_is_not_excluded():
    """The legacy in-product send predates the property, so its method is null.

    Without the coalesce, `null not in ('manual')` is UNKNOWN and every pre-property
    send drops out, which would silently delete the metric's own history.
    """
    sql = render_activation(True, declared_meta(ACTIVATION_METRIC))
    assert f"coalesce({METHOD_COL}, '')" in sql


def test_activation_parse_time_renders_the_false_fallback():
    assert render_activation(False, declared_meta(ACTIVATION_METRIC)) == "(false)"


def test_an_empty_activation_declaration_raises_rather_than_zeroing_the_metric():
    with pytest.raises(CompilerError):
        render_activation(True, {"anchored_on": []})


def test_an_exclusion_the_macro_cannot_compile_raises():
    """Silently ignoring an unknown qualifier would widen the metric without a trace."""
    with pytest.raises(CompilerError):
        render_activation(True, {"anchored_on": [{"event": "E", "excluding": {"channel": "sms"}}]})


def test_a_path_leg_on_the_activation_metric_raises():
    """This macro takes no page-path column, so a pathed leg must fail, not be dropped."""
    with pytest.raises(CompilerError):
        render_activation(True, {"anchored_on": [{"event": "E", "path": "/x"}]})


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
