-- Parity for the nine converted int__ballotready_* models against their
-- Python-built prod tables. Companion to parity_ballotready_geofence (that one
-- already shipped; this covers the other nine entities from the same conversion).
--
-- Must be run against the dev environment. `new_*` resolves through `ref()` into
-- a dev schema, but `old_*` is the hardcoded prod relation
-- goodparty_data_catalog.dbt.int__ballotready_<entity>. Point both at prod and
-- they resolve to the same object: the harness compares the new view against
-- itself and every entity reports perfect, meaningless agreement.
--
-- Two harness shapes, not one, because only three entities carry a timestamp that
-- can discriminate:
--
-- filing_period, candidacy, and person have a real `updated_at`, sourced from the
-- BallotReady payload. For these, `old_updated_at < new_updated_at` means the old
-- side was fetched before BallotReady last changed the record -- a stale API
-- snapshot, not a defect -- and lands in `old_side_stale`. A mismatch with equal
-- timestamps means the transform disagrees with the Python model on the same
-- input, and lands in `transform_bugs`. `unclassified` is the arithmetic residual
-- (not a third predicate), so it stays an exhaustive partition rather than a
-- plausible-looking one; it must be zero.
--
-- stance, endorsement, party, issue, normalized_position, and
-- position_election_frequency have no such timestamp. Five of them synthesise
-- `created_at`/`updated_at` as `current_timestamp()` on every run, so
-- `old_updated_at < new_updated_at` would be true by construction and every
-- difference would silently fall into `old_side_stale` -- the harness would
-- report success no matter how broken the transform was. The sixth,
-- position_election_frequency, has no `created_at`/`updated_at` at all. These six
-- exclude the timestamps from the comparison entirely and report differences as
-- findings (scalar_mismatch / array_size_mismatch /
-- array_content_mismatch_same_size) rather than bucketing them into an excuse.
-- Non-null-ness and type of the synthesised timestamps are asserted separately,
-- outside this harness.
--
-- candidacy and person also carry `feed_extracted_at`, a pipeline landing
-- timestamp on both sides (old: Airbyte sync time; new: the Airflow landing
-- watermark) rather than payload data. It is excluded from both harnesses'
-- column comparisons for the same reason as the six synthesised timestamps above:
-- it cannot agree by value regardless of transform correctness.
--
-- Arrays are compared with sort_array: element order carries no meaning, and the
-- two sides build each array by different routes.
with
    -- filing_period: real updated_at, no arrays.
    new_filing_period as (select * from {{ ref("int__ballotready_filing_period") }}),
    old_filing_period as (
        select * from goodparty_data_catalog.dbt.int__ballotready_filing_period
    ),
    both_filing_period as (
        select
            n.id,
            n.updated_at as new_updated_at,
            o.updated_at as old_updated_at,
            n.database_id = o.database_id
            and n.end_on <=> o.end_on
            and n.notes <=> o.notes
            and n.start_on <=> o.start_on
            and n.type <=> o.type
            and n.created_at <=> o.created_at
            and n.updated_at <=> o.updated_at as all_columns_match
        from new_filing_period n
        inner join old_filing_period o on n.id = o.id
    ),

    -- candidacy: real updated_at, three arrays, plus feed_extracted_at excluded.
    new_candidacy as (select * from {{ ref("int__ballotready_candidacy") }}),
    old_candidacy as (
        select * from goodparty_data_catalog.dbt.int__ballotready_candidacy
    ),
    both_candidacy as (
        select
            n.id,
            n.updated_at as new_updated_at,
            o.updated_at as old_updated_at,
            n.database_id = o.database_id
            and n.candidate_database_id <=> o.candidate_database_id
            and n.election_database_id <=> o.election_database_id
            and n.is_certified <=> o.is_certified
            and n.is_hidden <=> o.is_hidden
            and n.position_database_id <=> o.position_database_id
            and n.race_database_id <=> o.race_database_id
            and n.result <=> o.result
            and n.withdrawn <=> o.withdrawn
            and n.created_at <=> o.created_at
            and n.updated_at <=> o.updated_at
            and sort_array(n.endorsements) <=> sort_array(o.endorsements)
            and sort_array(n.parties) <=> sort_array(o.parties)
            and sort_array(n.stances) <=> sort_array(o.stances) as all_columns_match
        from new_candidacy n
        inner join old_candidacy o on n.id = o.id
    ),

    -- person: real updated_at, seven arrays, plus feed_extracted_at excluded.
    new_person as (select * from {{ ref("int__ballotready_person") }}),
    old_person as (select * from goodparty_data_catalog.dbt.int__ballotready_person),
    both_person as (
        select
            n.id,
            n.updated_at as new_updated_at,
            o.updated_at as old_updated_at,
            n.database_id = o.database_id
            and n.bio_text <=> o.bio_text
            and n.first_name <=> o.first_name
            and n.full_name <=> o.full_name
            and n.last_name <=> o.last_name
            and n.middle_name <=> o.middle_name
            and n.nickname <=> o.nickname
            and n.slug <=> o.slug
            and n.suffix <=> o.suffix
            and n.created_at <=> o.created_at
            and n.updated_at <=> o.updated_at
            and sort_array(n.candidacies) <=> sort_array(o.candidacies)
            and sort_array(n.contacts) <=> sort_array(o.contacts)
            and sort_array(n.degrees) <=> sort_array(o.degrees)
            and sort_array(n.experiences) <=> sort_array(o.experiences)
            and sort_array(n.images) <=> sort_array(o.images)
            and sort_array(n.office_holders) <=> sort_array(o.office_holders)
            and sort_array(n.urls) <=> sort_array(o.urls) as all_columns_match
        from new_person n
        inner join old_person o on n.id = o.id
    ),

    -- stance: synthesised timestamps excluded, one array, no other data column.
    new_stance as (select * from {{ ref("int__ballotready_stance") }}),
    old_stance as (select * from goodparty_data_catalog.dbt.int__ballotready_stance),
    both_stance as (
        select
            n.candidacy_id,
            sort_array(n.stances) <=> sort_array(o.stances) as arrays_match,
            size(n.stances) as new_size,
            size(o.stances) as old_size
        from new_stance n
        inner join old_stance o on n.candidacy_id = o.candidacy_id
    ),

    -- endorsement: same shape as stance.
    new_endorsement as (select * from {{ ref("int__ballotready_endorsement") }}),
    old_endorsement as (
        select * from goodparty_data_catalog.dbt.int__ballotready_endorsement
    ),
    both_endorsement as (
        select
            n.candidacy_id,
            sort_array(n.endorsements) <=> sort_array(o.endorsements) as arrays_match,
            size(n.endorsements) as new_size,
            size(o.endorsements) as old_size
        from new_endorsement n
        inner join old_endorsement o on n.candidacy_id = o.candidacy_id
    ),

    -- party: same shape as stance.
    new_party as (select * from {{ ref("int__ballotready_party") }}),
    old_party as (select * from goodparty_data_catalog.dbt.int__ballotready_party),
    both_party as (
        select
            n.candidacy_id,
            sort_array(n.parties) <=> sort_array(o.parties) as arrays_match,
            size(n.parties) as new_size,
            size(o.parties) as old_size
        from new_party n
        inner join old_party o on n.candidacy_id = o.candidacy_id
    ),

    -- issue: synthesised timestamps excluded, no arrays.
    new_issue as (select * from {{ ref("int__ballotready_issue") }}),
    old_issue as (select * from goodparty_data_catalog.dbt.int__ballotready_issue),
    both_issue as (
        select
            n.id,
            n.database_id = o.database_id
            and n.key <=> o.key
            and n.name <=> o.name
            and n.plugin_enabled <=> o.plugin_enabled
            and n.response_type <=> o.response_type
            and n.row_order <=> o.row_order as all_columns_match
        from new_issue n
        inner join old_issue o on n.id = o.id
    ),

    -- normalized_position: synthesised timestamps excluded, one array plus
    -- scalars, so scalar and array mismatches are tracked separately (they are
    -- not mutually exclusive -- a row can carry both at once).
    new_normalized_position as (
        select * from {{ ref("int__ballotready_normalized_position") }}
    ),
    old_normalized_position as (
        select * from goodparty_data_catalog.dbt.int__ballotready_normalized_position
    ),
    both_normalized_position as (
        select
            n.id,
            n.database_id = o.database_id
            and n.description <=> o.description
            and n.mtfcc <=> o.mtfcc
            and n.name <=> o.name as scalars_match,
            sort_array(n.issues) <=> sort_array(o.issues) as arrays_match,
            size(n.issues) as new_size,
            size(o.issues) as old_size
        from new_normalized_position n
        inner join old_normalized_position o on n.id = o.id
    ),

    -- position_election_frequency: no created_at/updated_at at all, so nothing
    -- to exclude. valid_from/valid_to are real payload data (a validity window),
    -- not run stamps, so they are compared like any other scalar. Two arrays.
    new_position_election_frequency as (
        select * from {{ ref("int__ballotready_position_election_frequency") }}
    ),
    old_position_election_frequency as (
        select *
        from goodparty_data_catalog.dbt.int__ballotready_position_election_frequency
    ),
    both_position_election_frequency as (
        select
            n.id,
            n.database_id = o.database_id
            and n.reference_year <=> o.reference_year
            and n.valid_from <=> o.valid_from
            and n.valid_to <=> o.valid_to as scalars_match,
            sort_array(n.frequency) <=> sort_array(o.frequency)
            and sort_array(n.seats) <=> sort_array(o.seats) as arrays_match,
            size(n.frequency) as new_frequency_size,
            size(o.frequency) as old_frequency_size,
            size(n.seats) as new_seats_size,
            size(o.seats) as old_seats_size
        from new_position_election_frequency n
        inner join old_position_election_frequency o on n.id = o.id
    )

-- filing_period, candidacy, person: the geofence-style classifier.
-- unclassified is the residual old_updated_at > new_updated_at (old side newer,
-- e.g. mid-backfill) or a null on one side only: both fail every predicate above
-- (< is UNKNOWN, <=> is false), so a predicate-based bucket would inherit the
-- same null semantics that created the gap. Must be zero for these three.
select
    'filing_period' as entity,
    (select count(*) from new_filing_period) as new_rows,
    (select count(*) from old_filing_period) as old_rows,
    (select count(*) from both_filing_period) as shared_ids,
    (select count(*) from both_filing_period where all_columns_match) as matching,
    (
        select count(*)
        from both_filing_period
        where not all_columns_match and old_updated_at < new_updated_at
    ) as old_side_stale,
    (
        select count(*)
        from both_filing_period
        where not all_columns_match and old_updated_at <=> new_updated_at
    ) as transform_bugs,
    (select count(*) from both_filing_period where not all_columns_match) - (
        select count(*)
        from both_filing_period
        where not all_columns_match and old_updated_at < new_updated_at
    )
    - (
        select count(*)
        from both_filing_period
        where not all_columns_match and old_updated_at <=> new_updated_at
    ) as unclassified,
    cast(null as bigint) as scalar_mismatch,
    cast(null as bigint) as array_size_mismatch,
    cast(null as bigint) as array_content_mismatch_same_size,
    -- unclassified already plays this role for the three classifier entities.
    cast(null as bigint) as residual_unaccounted,
    (
        select count(*)
        from new_filing_period
        where id not in (select id from old_filing_period where id is not null)
    ) as only_new,
    (
        select count(*)
        from old_filing_period
        where id not in (select id from new_filing_period where id is not null)
    ) as only_old

union all

select
    'candidacy' as entity,
    (select count(*) from new_candidacy) as new_rows,
    (select count(*) from old_candidacy) as old_rows,
    (select count(*) from both_candidacy) as shared_ids,
    (select count(*) from both_candidacy where all_columns_match) as matching,
    (
        select count(*)
        from both_candidacy
        where not all_columns_match and old_updated_at < new_updated_at
    ) as old_side_stale,
    (
        select count(*)
        from both_candidacy
        where not all_columns_match and old_updated_at <=> new_updated_at
    ) as transform_bugs,
    (select count(*) from both_candidacy where not all_columns_match) - (
        select count(*)
        from both_candidacy
        where not all_columns_match and old_updated_at < new_updated_at
    )
    - (
        select count(*)
        from both_candidacy
        where not all_columns_match and old_updated_at <=> new_updated_at
    ) as unclassified,
    cast(null as bigint) as scalar_mismatch,
    cast(null as bigint) as array_size_mismatch,
    cast(null as bigint) as array_content_mismatch_same_size,
    cast(null as bigint) as residual_unaccounted,
    (
        select count(*)
        from new_candidacy
        where id not in (select id from old_candidacy where id is not null)
    ) as only_new,
    (
        select count(*)
        from old_candidacy
        where id not in (select id from new_candidacy where id is not null)
    ) as only_old

union all

select
    'person' as entity,
    (select count(*) from new_person) as new_rows,
    (select count(*) from old_person) as old_rows,
    (select count(*) from both_person) as shared_ids,
    (select count(*) from both_person where all_columns_match) as matching,
    (
        select count(*)
        from both_person
        where not all_columns_match and old_updated_at < new_updated_at
    ) as old_side_stale,
    (
        select count(*)
        from both_person
        where not all_columns_match and old_updated_at <=> new_updated_at
    ) as transform_bugs,
    (select count(*) from both_person where not all_columns_match) - (
        select count(*)
        from both_person
        where not all_columns_match and old_updated_at < new_updated_at
    )
    - (
        select count(*)
        from both_person
        where not all_columns_match and old_updated_at <=> new_updated_at
    ) as unclassified,
    cast(null as bigint) as scalar_mismatch,
    cast(null as bigint) as array_size_mismatch,
    cast(null as bigint) as array_content_mismatch_same_size,
    cast(null as bigint) as residual_unaccounted,
    (
        select count(*)
        from new_person
        where id not in (select id from old_person where id is not null)
    ) as only_new,
    (
        select count(*)
        from old_person
        where id not in (select id from new_person where id is not null)
    ) as only_old

union all

-- stance, endorsement, party, issue, normalized_position,
-- position_election_frequency: no timestamp classifier. Differences are
-- reported as findings, not sorted into an excuse bucket.
select
    'stance' as entity,
    (select count(*) from new_stance) as new_rows,
    (select count(*) from old_stance) as old_rows,
    (select count(*) from both_stance) as shared_ids,
    (select count(*) from both_stance where arrays_match) as matching,
    cast(null as bigint) as old_side_stale,
    cast(null as bigint) as transform_bugs,
    cast(null as bigint) as unclassified,
    cast(0 as bigint) as scalar_mismatch,
    (
        select count(*)
        from both_stance
        where not arrays_match and not (new_size <=> old_size)
    ) as array_size_mismatch,
    (
        select count(*)
        from both_stance
        where not arrays_match and new_size <=> old_size
    ) as array_content_mismatch_same_size,
    -- Exhaustiveness check: stance has no scalar column and one array, so
    -- matching plus the two array buckets above must equal shared_ids exactly.
    -- A nonzero result here means a bucket predicate stopped being total (the
    -- defect a null-unsafe `=`/`!=` on an array size caused previously).
    (select count(*) from both_stance)
    - (select count(*) from both_stance where arrays_match)
    - (
        select count(*)
        from both_stance
        where not arrays_match and not (new_size <=> old_size)
    )
    - (
        select count(*)
        from both_stance
        where not arrays_match and new_size <=> old_size
    ) as residual_unaccounted,
    (
        select count(*)
        from new_stance
        where
            candidacy_id
            not in (select candidacy_id from old_stance where candidacy_id is not null)
    ) as only_new,
    (
        select count(*)
        from old_stance
        where
            candidacy_id
            not in (select candidacy_id from new_stance where candidacy_id is not null)
    ) as only_old

union all

select
    'endorsement' as entity,
    (select count(*) from new_endorsement) as new_rows,
    (select count(*) from old_endorsement) as old_rows,
    (select count(*) from both_endorsement) as shared_ids,
    (select count(*) from both_endorsement where arrays_match) as matching,
    cast(null as bigint) as old_side_stale,
    cast(null as bigint) as transform_bugs,
    cast(null as bigint) as unclassified,
    cast(0 as bigint) as scalar_mismatch,
    (
        select count(*)
        from both_endorsement
        where not arrays_match and not (new_size <=> old_size)
    ) as array_size_mismatch,
    (
        select count(*)
        from both_endorsement
        where not arrays_match and new_size <=> old_size
    ) as array_content_mismatch_same_size,
    (select count(*) from both_endorsement)
    - (select count(*) from both_endorsement where arrays_match)
    - (
        select count(*)
        from both_endorsement
        where not arrays_match and not (new_size <=> old_size)
    )
    - (
        select count(*)
        from both_endorsement
        where not arrays_match and new_size <=> old_size
    ) as residual_unaccounted,
    (
        select count(*)
        from new_endorsement
        where
            candidacy_id not in (
                select candidacy_id from old_endorsement where candidacy_id is not null
            )
    ) as only_new,
    (
        select count(*)
        from old_endorsement
        where
            candidacy_id not in (
                select candidacy_id from new_endorsement where candidacy_id is not null
            )
    ) as only_old

union all

select
    'party' as entity,
    (select count(*) from new_party) as new_rows,
    (select count(*) from old_party) as old_rows,
    (select count(*) from both_party) as shared_ids,
    (select count(*) from both_party where arrays_match) as matching,
    cast(null as bigint) as old_side_stale,
    cast(null as bigint) as transform_bugs,
    cast(null as bigint) as unclassified,
    cast(0 as bigint) as scalar_mismatch,
    (
        select count(*)
        from both_party
        where not arrays_match and not (new_size <=> old_size)
    ) as array_size_mismatch,
    (
        select count(*) from both_party where not arrays_match and new_size <=> old_size
    ) as array_content_mismatch_same_size,
    (select count(*) from both_party)
    - (select count(*) from both_party where arrays_match)
    - (
        select count(*)
        from both_party
        where not arrays_match and not (new_size <=> old_size)
    )
    - (
        select count(*) from both_party where not arrays_match and new_size <=> old_size
    ) as residual_unaccounted,
    (
        select count(*)
        from new_party
        where
            candidacy_id
            not in (select candidacy_id from old_party where candidacy_id is not null)
    ) as only_new,
    (
        select count(*)
        from old_party
        where
            candidacy_id
            not in (select candidacy_id from new_party where candidacy_id is not null)
    ) as only_old

union all

select
    'issue' as entity,
    (select count(*) from new_issue) as new_rows,
    (select count(*) from old_issue) as old_rows,
    (select count(*) from both_issue) as shared_ids,
    (select count(*) from both_issue where all_columns_match) as matching,
    cast(null as bigint) as old_side_stale,
    cast(null as bigint) as transform_bugs,
    cast(null as bigint) as unclassified,
    (select count(*) from both_issue where not all_columns_match) as scalar_mismatch,
    cast(0 as bigint) as array_size_mismatch,
    cast(0 as bigint) as array_content_mismatch_same_size,
    (select count(*) from both_issue)
    - (select count(*) from both_issue where all_columns_match)
    - (
        select count(*) from both_issue where not all_columns_match
    ) as residual_unaccounted,
    (
        select count(*)
        from new_issue
        where id not in (select id from old_issue where id is not null)
    ) as only_new,
    (
        select count(*)
        from old_issue
        where id not in (select id from new_issue where id is not null)
    ) as only_old

union all

select
    'normalized_position' as entity,
    (select count(*) from new_normalized_position) as new_rows,
    (select count(*) from old_normalized_position) as old_rows,
    (select count(*) from both_normalized_position) as shared_ids,
    (
        select count(*)
        from both_normalized_position
        where scalars_match and arrays_match
    ) as matching,
    cast(null as bigint) as old_side_stale,
    cast(null as bigint) as transform_bugs,
    cast(null as bigint) as unclassified,
    (
        select count(*) from both_normalized_position where not scalars_match
    ) as scalar_mismatch,
    (
        select count(*)
        from both_normalized_position
        where not arrays_match and not (new_size <=> old_size)
    ) as array_size_mismatch,
    (
        select count(*)
        from both_normalized_position
        where not arrays_match and new_size <=> old_size
    ) as array_content_mismatch_same_size,
    -- No arithmetic residual here: scalar_mismatch and the array buckets are
    -- not mutually exclusive for this entity (a row can fail both at once), so
    -- shared_ids - matching - scalar_mismatch - array buckets would double-count
    -- overlap rows rather than reveal an uncaptured one. matching itself is
    -- `scalars_match and arrays_match`, both already null-safe, so the
    -- top-line matching/mismatched split is not exposed to the bug this
    -- column would otherwise guard against.
    cast(null as bigint) as residual_unaccounted,
    (
        select count(*)
        from new_normalized_position
        where id not in (select id from old_normalized_position where id is not null)
    ) as only_new,
    (
        select count(*)
        from old_normalized_position
        where id not in (select id from new_normalized_position where id is not null)
    ) as only_old

union all

select
    'position_election_frequency' as entity,
    (select count(*) from new_position_election_frequency) as new_rows,
    (select count(*) from old_position_election_frequency) as old_rows,
    (select count(*) from both_position_election_frequency) as shared_ids,
    (
        select count(*)
        from both_position_election_frequency
        where scalars_match and arrays_match
    ) as matching,
    cast(null as bigint) as old_side_stale,
    cast(null as bigint) as transform_bugs,
    cast(null as bigint) as unclassified,
    (
        select count(*) from both_position_election_frequency where not scalars_match
    ) as scalar_mismatch,
    (
        select count(*)
        from both_position_election_frequency
        where
            not arrays_match
            and (
                not (new_frequency_size <=> old_frequency_size)
                or not (new_seats_size <=> old_seats_size)
            )
    ) as array_size_mismatch,
    (
        select count(*)
        from both_position_election_frequency
        where
            not arrays_match
            and new_frequency_size <=> old_frequency_size
            and new_seats_size <=> old_seats_size
    ) as array_content_mismatch_same_size,
    -- Same reason as normalized_position: scalar_mismatch and the array
    -- buckets overlap, so no arithmetic residual is provided.
    cast(null as bigint) as residual_unaccounted,
    (
        select count(*)
        from new_position_election_frequency
        where
            id
            not in (select id from old_position_election_frequency where id is not null)
    ) as only_new,
    (
        select count(*)
        from old_position_election_frequency
        where
            id
            not in (select id from new_position_election_frequency where id is not null)
    ) as only_old
