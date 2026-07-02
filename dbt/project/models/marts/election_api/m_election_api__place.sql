{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        auto_liquid_cluster=true,
    )
}}

with
    place_ids_in_races as (
        select distinct place_id from {{ ref("int__enhanced_race") }}
    ),
    parent_ids as (
        select distinct parent_id
        from {{ ref("int__enhanced_place_w_parent") }}
        where id in (select place_id from place_ids_in_races)
    ),
    grandparent_ids as (
        select distinct tbl_parent.parent_id as grandparent_id
        from {{ ref("int__enhanced_place_w_parent") }} as tbl_parent
        where tbl_parent.id in (select parent_id from parent_ids)
    ),
    greatgrandparent_ids as (
        select distinct tbl_parent.parent_id as greatgrandparent_id
        from {{ ref("int__enhanced_place_w_parent") }} as tbl_parent
        where tbl_parent.id in (select grandparent_id from grandparent_ids)
    ),
    -- The full place universe, deliberately not incremental-sliced: slug
    -- ownership below is a global property of the universe, so it must be
    -- recomputed over all rows every run. The incremental filter is applied
    -- to the outcome instead (see the final select).
    enriched_place_and_lineage as (
        select
            tbl_place.id,
            tbl_place.created_at,
            tbl_place.updated_at,
            tbl_place.br_database_id,
            replace(tbl_place.`name`, 'CCD', '') as name,
            replace(tbl_place.place_name_slug, '-ccd', '') as slug,
            tbl_place.geo_id as geoid,
            tbl_place.mtfcc,
            tbl_place.`state`,
            tbl_place.city_largest,
            tbl_place.county_name,
            tbl_place.population,
            tbl_place.density,
            tbl_place.income_household_median,
            tbl_place.unemployment_rate,
            tbl_place.home_value,
            tbl_place.parent_id
        from {{ ref("int__enhanced_place_w_parent") }} as tbl_place
        where
            tbl_place.geo_id is not null
            and tbl_place.place_name_slug is not null
            and tbl_place.name is not null
            and (
                tbl_place.id in (select place_id from place_ids_in_races)
                or tbl_place.id in (select parent_id from parent_ids)
                or tbl_place.id in (select grandparent_id from grandparent_ids)
                or tbl_place.id
                in (select greatgrandparent_id from greatgrandparent_ids)
            )
    ),
    -- One row per geography: the upstream place-with-parent model can carry a
    -- geoid more than once (e.g. fanned out across parent rows). The old
    -- slug-level dedup collapsed those incidentally; keep that guarantee
    -- explicit now that slug-collision losers are no longer dropped.
    deduped_by_geoid as (
        select *
        from enriched_place_and_lineage
        qualify
            row_number() over (
                partition by geoid order by updated_at desc, id, parent_id
            )
            = 1
    ),
    -- Distinct geographies can share a slug (e.g. a city and a same-named
    -- school district). election-api requires Place.slug to be globally
    -- unique, but dropping the collision losers orphans every race that
    -- points at them. Instead, keep one canonical owner on the clean slug
    -- and give every other member a deterministic, stable '-<geoid>' suffix.
    -- Ownership: an incorporated place (mtfcc G4110) wins its slug (the
    -- place-page URL a user expects); collisions with no incorporated place
    -- keep the previous winner (latest updated_at) so already-published
    -- slugs do not churn.
    slug_disambiguated as (
        select
            * except (slug),
            case
                when
                    row_number() over (
                        partition by slug
                        order by (mtfcc = 'G4110') desc, updated_at desc, id
                    )
                    = 1
                then slug
                else concat(slug, '-', geoid)
            end as slug
        from deduped_by_geoid
    )

select
    tbl_place.id,
    tbl_place.created_at,
    -- Bump updated_at when a row is new to the mart or its slug changed, so
    -- the election-api write model's incremental filter (updated_at greater
    -- than the postgres max) actually publishes it: recovered collision
    -- losers and re-slugged rows keep a source updated_at that predates the
    -- watermark and would otherwise never land.
    {% if is_incremental() %}
        case
            when tbl_prev.id is null or tbl_place.slug <> tbl_prev.slug
            then current_timestamp()
            else tbl_place.updated_at
        end as updated_at,
    {% else %} tbl_place.updated_at,
    {% endif %}
    tbl_place.br_database_id,
    tbl_place.name,
    tbl_place.slug,
    tbl_place.geoid,
    tbl_place.mtfcc,
    tbl_place.state,
    tbl_place.city_largest,
    tbl_place.county_name,
    tbl_place.population,
    tbl_place.density,
    tbl_place.income_household_median,
    tbl_place.unemployment_rate,
    tbl_place.home_value,
    tbl_place.parent_id
from slug_disambiguated as tbl_place
{% if is_incremental() %}
    left join {{ this }} as tbl_prev on tbl_place.id = tbl_prev.id
    where
        tbl_prev.id is null
        or tbl_place.slug <> tbl_prev.slug
        or tbl_place.updated_at > (select max(updated_at) from {{ this }})
{% endif %}
