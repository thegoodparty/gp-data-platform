"""The catalog dbt writes to, pinned against the macro that decides it.

dbt's own target is the prod catalog even on a development target, and deferral
depends on that: a dev run reads prod and writes its own schemas. Sending a
non-prod target to the `dev` catalog is one macro, and if it stops returning the
prod catalog for prod then a build writes into `dev`, or the reverse, with a
green run either way.

Note this macro decides the catalog only. Which schema a model lands in is
generate_schema_name's job, and its answer for a non-prod target collapses every
layer into the target's schema, so a marts model resolves to `dev.dbt_<user>`
rather than `dev.mart_civics`. That is why none of these tests assert a schema.
"""

from pathlib import Path

import jinja2

MACRO = Path(__file__).resolve().parents[1] / "project/macros/generate_database_name.sql"

# Plain substitution, not %-formatting: the body is full of braces.
TEMPLATE = (
    "{% macro generate_database_name(custom_database_name=none, node=none) -%}"
    "\n__BODY__\n"
    "{%- endmacro %}"
)


def _render(target_name: str, custom_database_name: str | None = None) -> str:
    """Render the macro's body the way dbt would, with one target."""
    body = MACRO.read_text()
    # The file is a single macro declaration; take what is between its tags.
    inner = body.split("-%}", 1)[1].rsplit("{%-", 1)[0]
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    env.globals["target"] = {"name": target_name}
    rendered = TEMPLATE.replace("__BODY__", inner)
    return env.from_string(rendered).module.generate_database_name(custom_database_name)


def test_prod_writes_to_the_prod_catalog():
    assert _render("prod") == "goodparty_data_catalog"


def test_every_other_target_writes_to_dev():
    for target_name in ("hugh", "dev", "ci", "default", "staging"):
        assert _render(target_name) == "dev", f"target {target_name} would write into the prod catalog"


def test_a_models_own_database_wins():
    """A model that names a database keeps it, on any target.

    The finance mart and anything else reaching outside the split relies on this.
    """
    assert _render("prod", "segment_storage") == "segment_storage"
    assert _render("hugh", "segment_storage") == "segment_storage"
