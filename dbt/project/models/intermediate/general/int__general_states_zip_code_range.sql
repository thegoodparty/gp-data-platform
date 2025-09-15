{{ config(materialized="view") }}

/*
View of states and their zip code ranges (inclusive). Zip code ranges are viewed as an array
with the first element being the lower bound and the second element being the upper bound.

Examples:
- CT: [06001, 06928]
- DE: [19701, 19980]
- DC: [20001, 20037]
- ...

It is constructed by removing the square brackets and splitting the string on the comma.

This view is used to determine if a zip code is in a state's range (inclusive).
*/
select
    state_postal_code,
    split(regexp_replace(zip_code_range, '\\[|\\]', ''), ',') as zip_code_range
from {{ ref("states_zip_code_range") }}
