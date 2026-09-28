-- Forwarding addresses for retired person ids (election-api "PersonMerge"
-- table). Grain: one row per gp_person_id the mint has produced since Person
-- went live that is no longer anyone's id, pointing at the id its minting
-- record carries today. A profile URL resolves on the first 8 hex of the id,
-- so a merged-away id would 404 without this row. The minting record's current
-- id is the terminal survivor by construction (chains compress, a split drops
-- the row); a minting record that vanished upstream stays a 404. Contract and
-- derivation: models/marts/civics/PERSON_ID_RETIREMENT_HANDOFF.md.
with
    minted as (
        select
            gp_person_id,
            minting_source_name || '|' || minting_source_id as minting_record_key,
            min(dbt_valid_from) as first_minted_at,
            max(dbt_valid_to) as last_minted_to
        from {{ ref("snapshot__int__civics_person_canonical_ids") }}
        group by 1, 2
    ),

    current_ids as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    retired as (
        select minted.*
        from minted
        left anti join current_ids on current_ids.gp_person_id = minted.gp_person_id
    ),

    -- Slug as last published and the run that closed the id's last row. An id
    -- retired before this snapshot began has no row here and forwards with a
    -- null slug, which the API treats as losing a prefix collision rather than
    -- winning it.
    published as (
        select
            id,
            max_by(slug, dbt_valid_from) as last_slug,
            max(dbt_valid_to) as unpublished_at
        from {{ ref("snapshot__m_election_api__person") }}
        group by 1
    ),

    slug_history as (
        select min(dbt_valid_from) as started_at
        from {{ ref("snapshot__m_election_api__person") }}
    )

select
    retired.gp_person_id as retired_id,
    survivor.gp_person_id as surviving_id,
    published.last_slug as retired_slug,
    -- The later of leaving Person and leaving the mint (greatest skips nulls).
    -- An id can drop out of the public mart while still canonical and only
    -- retire later; stamping that earlier exit would put the row behind the
    -- feed consumer's cursor.
    greatest(published.unpublished_at, retired.last_minted_to) as retired_at,
    -- build timestamp: the table is swap-replaced wholesale each run
    current_timestamp() as created_at
from retired
inner join current_ids as survivor on survivor.record_key = retired.minting_record_key
-- A survivor missing from Person would redirect to a 404, so the row waits
-- until the survivor is published.
inner join
    {{ ref("m_election_api__person") }} as person on person.id = survivor.gp_person_id
left join published on published.id = retired.gp_person_id
cross join slug_history
-- Only ids that were actually published get a forwarding row; alias ids that
-- were never live would be noise. An id minted before slug history began may
-- have been published and left the mart before we could see it, so every such
-- id is kept when it retires, and the filter applies to ids minted afterwards.
-- With no slug history at all, everything is kept.
where
    published.id is not null
    or slug_history.started_at is null
    or retired.first_minted_at < slug_history.started_at
