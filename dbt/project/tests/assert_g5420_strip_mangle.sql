-- DATA-1950 (G5420): pins the suffix-strip contract so an edit that mangles place
-- names (e.g.
-- eats interior "Union", or breaks coded "X County R-V") fails loudly. MUST mirror
-- the model's
-- g5420_candidates/g5420_candidates2 expressions exactly; if you change the model
-- strip, change this.
with
    t(name, expected_city_cand, expected_county_cand) as (
        values
            ('Mount Union Area School District', 'mount union', cast(null as string)),
            ('Region 10 School District', 'region', cast(null as string)),
            ('Columbia 93 School District', 'columbia', cast(null as string)),
            (
                'Dodge City Unified School District 443',
                'dodge city',
                cast(null as string)
            ),
            (
                'Amagansett Union Free School District',
                'amagansett',
                cast(null as string)
            ),
            (
                'San Diego City Unified School District',
                'san diego city',
                cast(null as string)
            ),
            ('Canton R-V School District', 'canton', cast(null as string)),
            ('Brown County Community Unit School District 1', 'brown county', 'brown'),
            ('Echols County R-V School District', 'echols county', 'echols')
    ),
    base as (
        select
            name,
            expected_city_cand,
            expected_county_cand,
            lower(
                trim(
                    regexp_replace(
                        regexp_replace(name, ' *\\([^)]*\\) *$', ''), ' +', ' '
                    )
                )
            ) as n0
        from t
    ),
    stripped as (
        select
            name,
            expected_city_cand,
            expected_county_cand,
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            n0,
                            ' ((no[.]?|number|#) ?)?([0-9][0-9a-z-]*|r-[ivxlc]+|re-?[0-9]+[a-z]?|[a-z]-[0-9]+[a-z]?) *$',
                            ''
                        ),
                        ' ((union free|community consolidated|community unit|community|consolidated|independent|unified|central|area|regional|elementary|high|joint|cooperative|special|graded|common|metropolitan|public) )?(school district|school districts|school corporation|public schools|schools|school|district) *$',
                        ''
                    ),
                    ' (r-[ivxlc]+|re-?[0-9]+[a-z]?|[a-z]-[0-9]+[a-z]?|[0-9]+) *$',
                    ''
                )
            ) as city_cand
        from base
    ),
    computed as (
        select
            name,
            expected_city_cand,
            expected_county_cand,
            city_cand,
            case
                when city_cand rlike ' county$'
                then trim(regexp_replace(city_cand, ' county$', ''))
            end as county_cand
        from stripped
    )
select name, city_cand, expected_city_cand, county_cand, expected_county_cand
from computed
where
    city_cand is distinct from expected_city_cand
    or county_cand is distinct from expected_county_cand
