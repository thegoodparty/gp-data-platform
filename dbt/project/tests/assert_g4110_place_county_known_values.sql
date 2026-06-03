-- dbt/project/tests/assert_g4110_place_county_known_values.sql
-- DATA-1950: fails if a known incorporated place is assigned the wrong county.
-- Stable, high-confidence ground truth (independent of the uscities name-match).
-- Keyed on geo_id (the stable, unique place identifier) — NOT name+state, which
-- can match multiple rows (a G4110 place and a same-name county-equivalent).
with
    expected as (
        select *
        from
            (
                values
                    ('0606308', '06037'),  -- Beverly Hills  -> Los Angeles
                    ('5182000', '51810'),  -- Virginia Beach -> Virginia Beach (independent city)
                    ('0603526', '06029'),  -- Bakersfield    -> Kern
                    ('4845384', '48215'),  -- McAllen        -> Hidalgo
                    ('0608954', '06037')  -- Burbank        -> Los Angeles
            ) as t(geo_id, expected_county_fips)
    )
select f.geo_id, f.name, f.county_fips, e.expected_county_fips
from {{ ref("int__place_fast_facts") }} as f
join expected as e on f.geo_id = e.geo_id
where f.county_fips is distinct from e.expected_county_fips
