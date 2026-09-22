-- Forwarding addresses for retired person ids (election-api "PersonMerge"
-- table). Grain: one row per gp_person_id the mint has produced since Person
-- went live that is no longer anyone's id, pointing at the id its minting
-- record carries today. A profile URL resolves on the first 8 hex of the id,
-- so a merged-away id would 404 without this row. The minting record's current
-- id is the terminal survivor by construction (chains compress, a split drops
-- the row); a minting record that vanished upstream stays a 404. retired_slug
-- is published null by agreement with the app team: it only breaks a tie when
-- a retired id shares its 8-hex prefix with a live person, and recording every
-- published slug forever was not worth that. Contract and derivation:
-- models/marts/civics/PERSON_ID_RETIREMENT_HANDOFF.md.
with
    minted as (
        select
            gp_person_id,
            minting_source_name || '|' || minting_source_id as minting_record_key,
            max(dbt_valid_to) as retired_at
        from {{ ref("snapshot__int__civics_person_canonical_ids") }}
        group by 1, 2
    )

select
    minted.gp_person_id as retired_id,
    survivor.gp_person_id as surviving_id,
    cast(null as string) as retired_slug,
    minted.retired_at,
    -- build timestamp: the table is swap-replaced wholesale each run
    current_timestamp() as created_at
from minted
inner join
    {{ ref("int__civics_person_canonical_ids") }} as survivor
    on survivor.record_key = minted.minting_record_key
-- A survivor missing from Person would redirect to a 404, so the row waits
-- until the survivor is published.
inner join
    {{ ref("m_election_api__person") }} as person on person.id = survivor.gp_person_id
-- The minting record is the earliest member of its own cluster, so it carries
-- the id it mints for as long as that id exists. A different id today means
-- the mint moved on and nothing else can still be carrying the old one.
where survivor.gp_person_id != minted.gp_person_id
