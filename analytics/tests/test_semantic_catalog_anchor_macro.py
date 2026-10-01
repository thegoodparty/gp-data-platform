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
PRODUCT_COL = "event_properties:product::string"

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
    + f"\n{{{{ is_outreach_activation_event('{EVENT_COL}', '{METHOD_COL}', '{PRODUCT_COL}') }}}}"
)
# The campaign-count call: drops the per-door and per-call legs.
CAMPAIGN_PREDICATE = ENV.from_string(
    macro_source("is_outreach_activation_event")
    + f"\n{{{{ is_outreach_activation_event('{EVENT_COL}', '{METHOD_COL}', '{PRODUCT_COL}',"
    + " contact_legs=false) }}"
)
# A caller that passes no product column, as every caller did before one existed.
NO_PRODUCT_PREDICATE = ENV.from_string(
    macro_source("is_outreach_activation_event")
    + f"\n{{{{ is_outreach_activation_event('{EVENT_COL}', '{METHOD_COL}') }}}}"
)
PRODUCT_OUTPUT_PREDICATE = ENV.from_string(
    macro_source("product_output_predicate")
    + f"\n{{{{ product_output_predicate('{EVENT_COL}', '{METHOD_COL}', 'caller', product_col='{PRODUCT_COL}') }}}}"
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


def render_activation(execute: bool, meta: dict, template=ACTIVATION_PREDICATE) -> str:
    sql = template.render(
        execute=execute,
        metric_anchored_events=lambda _name: anchored_events(execute, meta, ACTIVATION_METRIC),
    )
    return " ".join(sql.split())


def render_product_output(meta: dict) -> str:
    sql = PRODUCT_OUTPUT_PREDICATE.render(
        execute=True,
        metric_anchored_events=lambda _name: anchored_events(True, meta, "win_product_output_users"),
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


SERVE_EXCLUDED = {"anchored_on": [{"event": "E", "excluding": {"method": "manual", "product": "serve"}}]}
CONTACT_LEG = {"anchored_on": [{"event": "Send"}, {"event": "Door", "unit": "contact"}]}


def test_a_product_exclusion_renders_beside_the_method_one():
    """Serve outreach is excluded from Win by the emitted SQL, alongside self-report."""
    sql = render_activation(True, SERVE_EXCLUDED)
    assert (
        f"{EVENT_COL} = 'E' and coalesce({METHOD_COL}, '') not in ( 'manual' ) "
        f"and coalesce({PRODUCT_COL}, '') not in ( 'serve' )"
    ) in sql


def test_a_product_exclusion_keeps_events_that_predate_the_property():
    """Events before 2026-09-29 carry no product, and must not drop out of the metric."""
    assert f"coalesce({PRODUCT_COL}, '')" in render_activation(True, SERVE_EXCLUDED)


def test_a_product_exclusion_with_no_product_column_raises():
    """A caller that cannot filter on product must fail, not quietly count Serve."""
    with pytest.raises(CompilerError, match="passed no column"):
        render_activation(True, SERVE_EXCLUDED, NO_PRODUCT_PREDICATE)


def test_contact_legs_count_toward_activation():
    """A door knocked or a call logged is the user acting, so it activates them."""
    assert "'Door'" in render_activation(True, CONTACT_LEG)


def test_campaign_counts_drop_contact_legs():
    """Every call logged would otherwise read as one campaign sent."""
    sql = render_activation(True, CONTACT_LEG, CAMPAIGN_PREDICATE)
    assert "'Send'" in sql
    assert "'Door'" not in sql


def test_an_unknown_unit_raises():
    with pytest.raises(CompilerError, match="unit"):
        render_activation(True, {"anchored_on": [{"event": "E", "unit": "household"}]})


def test_every_model_counting_campaigns_drops_contact_legs():
    """The count columns are the only callers that may drop them, and all of them must.

    Timestamps keep contact legs: `users_win_base.is_activated` is
    `first_campaign_sent_at is not null`, so dropping them there would undo the rule.
    """
    counts = {"campaigns_sent", "recipient_count", "total_campaigns_sent", "total_recipient_count"}
    for name in (
        "int__amplitude_user_milestones",
        "int__amplitude_win_activity",
        "int__amplitude_win_activity_weekly",
    ):
        src = (MODELS / "intermediate/amplitude" / f"{name}.sql").read_text()
        for call in re.finditer(r"is_outreach_activation_event\((.*?)\)\s*\}\}.*?\) as (\w+),", src, re.S):
            drops = "contact_legs=false" in call.group(1)
            assert drops == (call.group(2) in counts), f"{name}.{call.group(2)}"


def test_the_product_output_predicate_compiles_a_product_exclusion():
    sql = render_product_output(SERVE_EXCLUDED)
    assert f"and coalesce({PRODUCT_COL}, '') not in ( 'serve' )" in sql


def test_the_milestone_filter_admits_every_declared_anchor_event():
    """The filter upstream of the predicate must not drop an event the metrics anchor on.

    Scoped to the metrics this model actually serves. `win_product_output_users` is
    anchored too but is computed in `int__user_product_activity`, which admits events
    by classification rather than by name; its own intake guard is the machine-emitted
    test below.
    """
    admitted = milestone_filter()
    served = (METRIC, ACTIVATION_METRIC, "activated_serve_users")
    missing = [
        leg["event"]
        for metric, legs in declared_legs().items()
        if metric in served
        for leg in legs
        if f"'{leg['event']}'" not in admitted
    ]
    assert not missing, f"declared but dropped by milestone_events: {missing}"


def test_no_product_output_leg_is_classified_machine_emitted():
    """The product-output model admits events by classification, so its intake gate is
    the machine-emitted flag rather than a name list. A leg the classifier catches
    would be dropped before the predicate ever saw it, and the column would quietly
    under-count instead of failing.
    """
    machine = ENV.from_string(
        macro_source("amplitude_event_is_machine_emitted")
        + '\n{{ amplitude_event_is_machine_emitted("\'" ~ event ~ "\'") }}'
    )
    caught = []
    for leg in declared_legs()["win_product_output_users"]:
        rendered = " ".join(machine.render(event=leg["event"]).split())
        # The predicate is a disjunction of literal comparisons against the event
        # name, so it is decidable here without a warehouse: a leg is caught only
        # if one of its own literals matches.
        if f"= '{leg['event']}'" in rendered or f"'{leg['event']}'," in rendered:
            caught.append(leg["event"])
    assert not caught, f"product-output legs the machine classifier drops: {caught}"


def test_the_milestone_filter_keeps_the_path_leg_condition():
    """'Viewed' is site-wide; admitting it unconditionally would widen the filter hugely."""
    admitted = milestone_filter()
    for leg in (leg for leg in declared_legs()[METRIC] if leg["path"]):
        assert f"{EVENT_COL} = '{leg['event']}' and {PATH_COL} = '{leg['path']}'" in admitted
