"""Sources that declare no catalog resolve into the one dbt writes to.

That omission is invisible in review, since the missing line is not in the diff,
and on a prod build it puts a source in the catalog every reader can see. The
loader sources name raw explicitly; these are the ones that deliberately do not,
and everything else has to say where it lives.
"""

from pathlib import Path

import yaml

PROJECT = Path(__file__).resolve().parents[1] / "project"

# Sources read from outside Unity Catalog, or through a prod-catalog object that
# names its own catalog, so moving them would break what reads them.
SOURCES_WITHOUT_A_DATABASE = {
    # Sandbox tables another team maintains for a specific analysis.
    "nhgis",
    "census_acs",
    # gp-api's Postgres mirror, inside the prod catalog on purpose so the mart
    # that reads it stays queryable by the readers who can see that mart.
    "gp_api_pg",
    # The suppression register the deletion procedure writes.
    "source_dsar",
    # A personal schema, read from by one model that predates the split.
    "dbt_hugh",
    # Read through a prod-catalog object that names its own catalog: the masking
    # function reads the salt from goodparty_data_catalog.governance, and MLflow
    # records these tables by their prod-catalog path.
    "model_predictions",
}


def test_no_source_lands_in_the_prod_catalog_unless_it_says_so():
    """A source with no `database` key inherits the target's catalog.

    On a prod build that means it lands in prod, which is where the split has a
    hole in it. Naming the catalog, or listing the source above, is the fix.
    """
    config = yaml.safe_load((PROJECT / "models/staging/__sources.yaml").read_text())

    unresolved = sorted(
        source["name"]
        for source in config["sources"]
        if not source.get("database") and source["name"] not in SOURCES_WITHOUT_A_DATABASE
    )

    assert not unresolved, (
        "These sources name no database and are not on the list above, so they "
        f"resolve into whatever catalog dbt writes to: {unresolved}"
    )
