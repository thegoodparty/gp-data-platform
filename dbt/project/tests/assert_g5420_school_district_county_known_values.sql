-- DATA-1950 (G5420): fails if a known school district gets the wrong county, OR an
-- anchor row
-- is dropped from the model. Ground truth independent of the suffix-strip match.
-- geo_id-keyed.
-- LEFT JOIN from fixtures + `f.geo_id is null` also catches a dropped (incl.
-- null-expected) anchor.
with
    expected as (
        select *
        from
            (
                values
                    ('3602880', '36103'),  -- Amagansett UFSD         -> Suffolk NY
                    ('3603840', '36059'),  -- Baldwin UFSD            -> Nassau NY
                    ('3604080', '36103'),  -- Bay Shore UFSD          -> Suffolk NY
                    ('3606840', '36103'),  -- Center Moriches UFSD    -> Suffolk NY
                    ('3610080', '36119'),  -- Eastchester UFSD        -> Westchester NY
                    ('1727300', '17009'),  -- Brown County CUSD 1     -> Brown IL (county match)
                    ('2005580', '20057'),  -- Dodge City USD 443      -> Ford KS (city primary)
                    ('0634320', '06073'),  -- San Diego City Unified  -> San Diego CA (city retry)
                    ('4216020', '42061'),  -- Mount Union Area SD     -> Huntingdon PA
                    ('2400120', '24005'),  -- Baltimore County PS     -> Baltimore County MD (not city 24510)
                    ('5103270', '51159'),  -- Richmond County PS (VA)  -> Richmond County VA (not city 51760)
                    ('1301950', '13101'),  -- Echols County SD (GA)   -> Echols (population-less county kept by the FIPS-prefix filter)
                    ('2901000', '29019'),  -- Columbia 93 SD (MO)     -> Boone (MO "Town NN" district number stripped in pass 3)
                    ('1739090', cast(null as string)),  -- Cumberland CUSD 77      -> NULL (honest; v2)
                    ('1741690', cast(null as string))  -- Indian Prairie CUSD 204 -> NULL (honest; v2)
            ) as t(geo_id, expected_county_fips)
    )
select e.geo_id, f.name, f.county_fips, e.expected_county_fips
from expected as e
left join {{ ref("int__place_fast_facts") }} as f on f.geo_id = e.geo_id
where f.county_fips is distinct from e.expected_county_fips or f.geo_id is null
