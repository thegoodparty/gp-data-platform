-- Warns when an office-holder override seed row has become redundant because
-- BallotReady now carries the correction itself: an end override whose term
-- BallotReady already ends on or before the override date, or an added term
-- whose person BallotReady now has a term for on the same position. Remove the
-- reported rows from the seed.
{{ config(severity="warn") }}

select 'term_end_override' as seed, cast(o.br_office_holder_id as string) as seed_key
from {{ ref("office_holder_term_end_overrides") }} as o
inner join
    {{ ref("elected_official_terms") }} as t
    on o.br_office_holder_id = t.br_office_holder_id
where t.term_end_date <= o.end_at

union all

select
    'term_addition' as seed,
    concat_ws('|', a.gp_person_id, cast(a.br_position_id as string)) as seed_key
from {{ ref("office_holder_term_additions") }} as a
inner join
    {{ ref("elected_official_terms") }} as t
    on a.gp_person_id = t.gp_person_id
    and a.br_position_id = t.br_position_id
