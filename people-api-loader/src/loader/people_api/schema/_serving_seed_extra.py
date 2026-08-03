"""Hand-maintained IndexDefs merged onto the generated `_serving_seed`.

`extract-serving-structure` overwrites `_serving_seed.py` wholesale, so anything not on the
extraction-source cluster is lost on regeneration. Indexes from outside pg_catalog (e.g. omni
prisma migrations, which don't run on loader clusters) live here and merge in via
`schema_spec.indexes_for`.
"""

from __future__ import annotations

from loader.people_api.schema.index_specs import IndexDef

# Serving-only performance indexes, carried here because they don't come from a prisma migration
# that runs on loader-built clusters. Two families:
#   1. people-api name-search (first/last name): lower() expressions must match people-api's emitted
#      SQL exactly or the planner skips them. The b-trees use text_pattern_ops so LIKE-'prefix%' uses
#      them on the en_US.UTF-8 serving cluster (a default opclass can't) — a deliberate divergence
#      from the prisma index, since the loader is becoming the source of truth. The trgm GIN serves
#      the substring path no b-tree can.
#   2. party-registration substring filter (Parties_Description): a trgm GIN serving the gp-api
#      contacts ILIKE audience filter (see the IndexDef comment below).
# pg_trgm is installed by create_schema and build_indexes (each step is independently re-runnable).
EXTRA_INDEXES: list[IndexDef] = [
    IndexDef(
        table="Voter",
        name="Voter_firstname_lower_idx",
        sql='CREATE INDEX "Voter_firstname_lower_idx" ON public."Voter" USING btree (lower("FirstName") text_pattern_ops);',
        unique=False,
        columns=['lower("FirstName")'],
        where=None,
    ),
    IndexDef(
        table="Voter",
        name="Voter_firstname_lower_trgm_idx",
        sql='CREATE INDEX "Voter_firstname_lower_trgm_idx" ON public."Voter" USING gin (lower("FirstName") gin_trgm_ops);',
        unique=False,
        columns=['lower("FirstName")'],
        where=None,
    ),
    IndexDef(
        table="Voter",
        name="Voter_last_first_id_idx",
        sql='CREATE INDEX "Voter_last_first_id_idx" ON public."Voter" USING btree ("LastName", "FirstName", "id");',
        unique=False,
        columns=["LastName", "FirstName", "id"],
        where=None,
    ),
    IndexDef(
        table="Voter",
        name="Voter_lastname_lower_idx",
        sql='CREATE INDEX "Voter_lastname_lower_idx" ON public."Voter" USING btree (lower("LastName") text_pattern_ops);',
        unique=False,
        columns=['lower("LastName")'],
        where=None,
    ),
    IndexDef(
        table="Voter",
        name="Voter_lastname_lower_trgm_idx",
        sql='CREATE INDEX "Voter_lastname_lower_trgm_idx" ON public."Voter" USING gin (lower("LastName") gin_trgm_ops);',
        unique=False,
        columns=['lower("LastName")'],
        where=None,
    ),
    # Party-registration substring filter. gp-api compiles a party audience to
    # `"Parties_Description" ILIKE '%<substring>%'` (case-insensitive, ORed per rule —
    # gp-api/src/peopleDb/utils/filters.sql.util.ts). The generated seed only carries a
    # plain b-tree on this column (Voter_Parties_Description_idx), which a substring ILIKE
    # can't use, so the contacts count/overlap-count/list-detail aggregates fall back to a
    # full Voter scan and hit the app's statement-timeout fence. A trgm GIN on the raw column
    # serves ILIKE directly (pg_trgm handles the case-folding), the same substring path the
    # name-search trgm indexes above serve. On the raw column, not lower(), to match the
    # emitted SQL exactly. Carried here (not the generated seed) because it doesn't yet exist
    # on the extraction-source cluster; build_indexes re-issues it on every rebuild.
    IndexDef(
        table="Voter",
        name="Voter_Parties_Description_trgm_idx",
        sql='CREATE INDEX "Voter_Parties_Description_trgm_idx" ON public."Voter" USING gin ("Parties_Description" gin_trgm_ops);',
        unique=False,
        columns=["Parties_Description"],
        where=None,
    ),
    # Geospatial lookups on the residence coordinates. GiST is the standard PostGIS point index:
    # fast to build in bulk and fast to probe (unlike BRIN, which needs spatially-sorted data). The
    # "geom" column it indexes is created by build_indexes (postgis + the generated column) before
    # this builds. pg_trgm and postgis are installed by create_schema and build_indexes.
    IndexDef(
        table="Voter",
        name="Voter_geom_idx",
        sql='CREATE INDEX "Voter_geom_idx" ON public."Voter" USING gist ("geom");',
        unique=False,
        columns=["geom"],
        where=None,
    ),
    IndexDef(
        table="Voter",
        name="Voter_hf_most_important_policy_item_idx",
        sql='CREATE INDEX "Voter_hf_most_important_policy_item_idx" ON public."Voter" USING btree ("hf_most_important_policy_item");',
        unique=False,
        columns=["hf_most_important_policy_item"],
        where=None,
    ),
    # Voter-density heat map: the app's only query pattern is filter by district + resolution
    # (people-api voter-density serve query, handoff §7/§8). DistrictVoterDensity is a `green`-schema
    # serving table absent from the extraction-source cluster, so its index is carried here (like the
    # Voter perf indexes) rather than the generated seed; build_indexes builds it directly (flat
    # table, no partitions). DistrictVoterDensityMeta needs no extra index — its PK
    # (district_id, resolution) already covers the same lookup.
    IndexDef(
        table="DistrictVoterDensity",
        name="DistrictVoterDensity_district_id_resolution_idx",
        sql='CREATE INDEX "DistrictVoterDensity_district_id_resolution_idx" ON public."DistrictVoterDensity" USING btree ("district_id", "resolution");',
        unique=False,
        columns=["district_id", "resolution"],
        where=None,
    ),
]
