"""Hand-maintained IndexDefs merged onto the generated `_serving_seed`.

`extract-serving-structure` overwrites `_serving_seed.py` wholesale, so anything not on the
extraction-source cluster is lost on regeneration. Indexes from outside pg_catalog (e.g. omni
prisma migrations, which don't run on loader clusters) live here and merge in via
`schema_spec.indexes_for`.
"""

from __future__ import annotations

from loader.people_api.schema.index_specs import IndexDef

# people-api name-search indexes, carried here because prisma migrations don't run on
# loader-built clusters. lower() expressions must match people-api's emitted SQL exactly or the
# planner skips them. The b-trees use text_pattern_ops so LIKE-'prefix%'
# uses them on the en_US.UTF-8 serving cluster (a default opclass can't) — a deliberate divergence
# from the prisma index, since the loader is becoming the source of truth. The trgm GIN serves the
# substring path no b-tree can. pg_trgm is installed by create_schema and build_indexes (each step
# is independently re-runnable).
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
    IndexDef(
        table="Voter",
        name="Voter_hf_most_important_policy_item_idx",
        sql='CREATE INDEX "Voter_hf_most_important_policy_item_idx" ON public."Voter" USING btree ("hf_most_important_policy_item");',
        unique=False,
        columns=["hf_most_important_policy_item"],
        where=None,
    ),
]
