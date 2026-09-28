{#
    Shared CTE body mapping every L2 district spelling to the district id that
    actually carries voters.

    L2 rewrites district names between snapshots, so m_election_api__district
    ends up holding two rows for one place: the spelling the current L2
    aggregation emits (which has registered_voters and DistrictVoter rows) and
    the spelling the frozen legacy turnout feeds keep alive (which has
    neither). Consumers that key on the name string are frozen on whichever
    spelling was current when they were written, so they land on the empty row
    and every voter-backed surface reads zero.

    A voterless spelling therefore adopts its populated sibling's id, but only
    where exactly one sibling is populated: a handful of places really are two
    distinct districts under both spellings and must stay separate.

    Returns one row per (state, l2_district_type, l2_district_name), the grain
    the district's salted id is built from, so joining on the name stays 1:1.
#}
{% macro l2_district_spelling_resolution() %}
    select
        state,
        l2_district_type,
        l2_district_name,
        case
            when registered_voters is not null
            then id
            when
                sum(case when registered_voters is not null then 1 else 0 end) over w
                = 1
            then max(case when registered_voters is not null then id end) over w
            else id
        end as district_id
    from {{ ref("m_election_api__district") }}
    window
        w as (
            partition by
                state,
                l2_district_type,
                {{ normalize_l2_district_name("l2_district_name") }}
        )
{% endmacro %}
