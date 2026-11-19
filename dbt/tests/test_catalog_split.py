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
# These name the raw catalog as a literal. Reads only move once the source
# schemas have been copied into raw and the loaders write there, so the literal
# and the loader change land together.
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


def test_every_loader_source_declares_the_raw_catalog():
    sources = _sources_by_name()

    missing = sorted(name for name in SOURCES_IN_RAW if sources[name].get("database") != "raw")

    assert not missing, (
        "These sources must name raw, or a build reads them from the catalog dbt "
        f"writes to instead: {missing}"
    )


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
    that has neither the function nor the registered model.
    """
    sources = _sources_by_name()

    moved = sorted(name for name in SOURCES_STAYING_IN_PROD if sources[name].get("database") == "raw")

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
