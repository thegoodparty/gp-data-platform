"""The catalog split, pinned as configuration rather than as behavior.

dbt reads one catalog and writes another, which nothing in a build checks: a
source that loses its `database` key resolves into the catalog dbt writes to,
which is where every masked reader can see it. These tests read the source and
model configuration directly, so a missing key fails here instead of exposing a
schema in production.
"""

from pathlib import Path

import yaml

PROJECT = Path(__file__).resolve().parents[1] / "project"

# Sources that resolve out of the catalog dbt builds in. Everything a loader
# writes belongs here; a new loader that lands in the prod catalog fails
# test_no_loader_source_lands_in_the_prod_catalog below.
#
# These name the catalog through the source_catalog var rather than a literal,
# because the loader side is a cutover rather than a flip: pointing them at raw
# before the source schemas are copied there would rebuild every mart from
# nothing. The var is what makes the cutover one value.
SOURCES_IN_RAW = {
    "airbyte_source",
    "dbt_source",
    "dbt_source_haystaq",
    "airflow_source",
    "er_source",
    "archives",
}

# Sources that deliberately live in another catalog entirely.
SOURCES_IN_OTHER_CATALOGS = {
    "segment_storage_source": "segment_storage",
    "segment_storage_source_web_app": "segment_storage",
}

# Sources read from outside Unity Catalog, or that predate the split and have no
# business moving with it.
SOURCES_EXEMPT = {
    # Sandbox tables another team maintains for a specific analysis.
    "nhgis",
    "census_acs",
    # gp-api's Postgres mirror, inside the prod catalog on purpose so the mart
    # that reads it stays queryable by the readers who can see that mart.
    "gp_api_pg",
    # The suppression register the deletion procedure writes.
    "source_dsar",
    # See SOURCES_STAYING_IN_PROD: read through a prod-catalog object.
    "model_predictions",
    # A personal schema, read from by one model that predates the split.
    "dbt_hugh",
}


# Sources that must stay in the prod catalog even though a loader writes them,
# because the thing that reads them is governed and cannot follow them into raw:
# a Unity Catalog model registry records the catalog it lives in, and the salt
# sits next to the masking functions the policies name.
SOURCES_STAYING_IN_PROD = {
    # sgd-import: MLflow registers models as goodparty_data_catalog.model_predictions.<name>
    "model_predictions",
}


def _sources_by_name() -> dict[str, dict]:
    config = yaml.safe_load((PROJECT / "models/staging/__sources.yaml").read_text())
    return {source["name"]: source for source in config["sources"]}


def test_every_loader_source_declares_the_source_catalog_var():
    sources = _sources_by_name()

    missing = sorted(
        name for name in SOURCES_IN_RAW if "source_catalog" not in str(sources[name].get("database"))
    )

    assert not missing, (
        "These sources must name the source catalog through the var, so the "
        f"cutover is one value rather than seven edits: {missing}"
    )


def test_the_source_catalog_var_starts_where_the_loaders_write():
    """The var must start on the catalog the loaders actually write to.

    Pointing it at raw before the copy has run is the failure this guards: the
    build succeeds, reads empty sources, and rebuilds every mart from nothing.
    It moves to raw in the same change that moves the loaders.
    """
    project = yaml.safe_load((PROJECT / "dbt_project.yml").read_text())

    assert project["vars"]["source_catalog"] == "goodparty_data_catalog"


def test_sources_in_other_catalogs_keep_their_own_database():
    sources = _sources_by_name()

    wrong = {
        name: sources[name].get("database")
        for name, catalog in SOURCES_IN_OTHER_CATALOGS.items()
        if sources[name].get("database") != catalog
    }

    assert not wrong, f"These sources must name their own catalog: {wrong}"


def test_governed_sources_stay_in_the_prod_catalog():
    """These read from prod objects that name their own catalog.

    Moving them to raw would point a policy or an MLflow model path at a catalog
    that has neither the function nor the registered model, and the source_catalog
    var must not sweep them along when it flips.
    """
    sources = _sources_by_name()

    moved = sorted(
        name
        for name in SOURCES_STAYING_IN_PROD
        if "source_catalog" in str(sources[name].get("database")) or sources[name].get("database") == "raw"
    )

    assert not moved, (
        "These sources are read through a prod-catalog object that names its own "
        f"catalog, so they cannot move to raw: {moved}"
    )


def test_no_source_lands_in_the_prod_catalog():
    """The catch-all: a source nobody listed above still has to resolve.

    A source with no `database` key inherits the target's catalog, so on a prod
    build it lands in prod and the split has a hole in it.
    """
    sources = _sources_by_name()

    unresolved = sorted(
        name for name, source in sources.items() if not source.get("database") and name not in SOURCES_EXEMPT
    )

    assert not unresolved, (
        "These sources name no database and are not on the exempt list, so they "
        f"resolve into the catalog dbt writes to: {unresolved}"
    )
