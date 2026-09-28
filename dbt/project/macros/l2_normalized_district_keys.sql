{#
    Normalized-key census for an L2 district-name relation: one row per
    (state, type, normalized name) with how many raw spellings share the key.

    `spellings = 1` is the reusable "unambiguous" predicate: a key carried by
    two real districts (same-name twins) never resolves, so a consumer can
    tolerate L2's respellings without ever crossing districts. Blank keys (a
    name that is only zero padding or an (EST.) marker) are excluded: one lone
    such row must not make every blank-normalizing label resolvable.

    The relation must carry state_postal_code, district_type, district_name --
    the shared shape of int__l2_district_universe and int__zip_code_to_l2_district.
#}
{% macro l2_normalized_district_keys(relation) %}
    select
        state_postal_code,
        district_type,
        {{ normalize_l2_district_name("district_name") }} as normalized_district_name,
        count(distinct district_name) as spellings
    from {{ relation }}
    where {{ normalize_l2_district_name("district_name") }} != ''
    group by
        state_postal_code,
        district_type,
        {{ normalize_l2_district_name("district_name") }}
{% endmacro %}
