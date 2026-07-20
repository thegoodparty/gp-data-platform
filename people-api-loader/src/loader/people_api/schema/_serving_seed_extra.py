"""Hand-maintained IndexDefs merged on top of the generated `_serving_seed`.

`loader extract-serving-structure` overwrites `_serving_seed.py` wholesale
(`render_seed_module` + `write_text`, no merge step), so any entry that does
not exist on the extraction-source cluster is silently lost on regeneration.
Indexes that originate outside pg_catalog extraction — e.g. omni-side indexes,
since people-api prisma migrations do not run against loader-built clusters —
live here instead and are merged at the consumption site
(`schema_spec.indexes_for`).
"""

from __future__ import annotations

from loader.people_api.schema.index_specs import IndexDef

# omni ENG-10684 (2026-07-16): the CRM typeahead's trigram substring search plus
# the name-search/ordering indexes that previously existed only as people-api
# prisma migrations. The lower() expressions must match the SQL people-api
# emits (lower("FirstName") / lower("LastName")) exactly or the planner will
# not use them. pg_trgm is installed by BOTH create_schema and build_indexes —
# the steps guard via independent manifests and are independently re-runnable.
#
# The lower() b-trees carry the text_pattern_ops opclass so they serve the
# anchored-prefix path (lower(col) LIKE 'tok%') people-api emits: a default
# opclass b-tree only serves LIKE-prefix in a C-locale DB, and the serving
# cluster is en_US.UTF-8, so without text_pattern_ops those b-trees would be
# dead weight and prefix search would fall back to the trigram GIN. This
# deliberately diverges from the current people-api prisma index (plain
# opclass); the loader/serving cluster is the source of truth, and the prisma
# controls are slated for deprecation. The trgm GIN indexes still serve the
# substring path (lower(col) LIKE '%tok%'), which no b-tree can.
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
]
