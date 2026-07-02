-- The nationwide file must contain exactly the full set of US states (incl DC):
-- none missing, none unexpected. Replaces the column-level equal-set test moved
-- off the shared column anchor for int__l2_nationwide_uniform_w_haystaq.
{% set states = get_us_states_list(include_DC=true, include_US=false) %}
with
    actual as (
        select distinct state_postal_code as s
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
        where state_postal_code is not null
    ),
    expected as (select explode(array('{{ states | join("', '") }}')) as s)
select 'missing_from_file' as issue, s
from expected
where s not in (select s from actual)
union all
select 'unexpected_in_file' as issue, s
from actual
where s not in (select s from expected)
