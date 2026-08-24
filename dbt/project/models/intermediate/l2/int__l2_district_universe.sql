-- Explicit because this directory sets no materialization default: an empty
-- config block would silently make this a view, and a view lets the candidate
-- set change underneath a run that has already pinned it.
{{ config(materialized="table") }}

/*
One row per district in each state's latest L2 delivery, plus 51 synthetic
statewide rows (district_type='State', district_name=state_postal_code) so
Governor and US Senate seats, which have no L2 district of their own, have
something to match to.

Filtering to each state's latest delivery is load-bearing, not hygiene: the
merge table underneath int__l2_nationwide_uniform never deletes a voter who
has since left the file, so an unfiltered rebuild would resurrect districts
L2 no longer carries.
*/
with
    latest_delivery_per_state as (
        select state_postal_code, max(loaded_at) as loaded_at
        from {{ ref("int__l2_nationwide_uniform") }}
        group by state_postal_code
    ),

    current_l2_data as (
        select
            uniform.state_postal_code,
            uniform.loaded_at,
            {{ get_l2_district_columns(use_backticks=true, cast_to_string=true) }}
        from {{ ref("int__l2_nationwide_uniform") }} as uniform
        inner join
            latest_delivery_per_state
            on uniform.state_postal_code = latest_delivery_per_state.state_postal_code
            and uniform.loaded_at = latest_delivery_per_state.loaded_at
    ),

    current_districts as (
        select distinct
            state_postal_code,
            district_column_name as district_type,
            district_value as district_name,
            loaded_at
        from
            current_l2_data unpivot (
                district_value for district_column_name
                in ({{ get_l2_district_columns(use_backticks=false) }})
            )
        where district_value is not null
    ),

    -- L2 has no "the whole state" column, so Governor and US Senate seats
    -- match this synthetic row instead of a real L2 district. Reads the
    -- delivery CTE rather than the voter file again: same group-by, same
    -- aggregate, one fewer pass over 219M rows.
    statewide_districts as (
        select
            state_postal_code,
            'State' as district_type,
            state_postal_code as district_name,
            loaded_at
        from latest_delivery_per_state
    )

select state_postal_code, district_type, district_name, loaded_at
from current_districts

union all

select state_postal_code, district_type, district_name, loaded_at
from statewide_districts
