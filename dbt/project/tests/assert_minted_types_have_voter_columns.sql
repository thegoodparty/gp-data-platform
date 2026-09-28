-- Every minted district type must be a column on the voter mart, or
-- m_people_api__districtvoter emits no links for it: it intersects voter columns
-- with district types. Those districts then carry a registered_voters count and
-- serve nobody, which reads as a filter bug rather than missing data.
--
-- assert_voter_mart_carries_every_l2_district_type covers the same ground for L2
-- types, but drives off get_l2_district_types(), which excludes minted types.
--
-- Compile-time against the mart's real columns, so it costs no scan and is immune
-- to the delivery lag that makes a row-level check flap.
-- depends_on: {{ ref('m_people_api__voter') }}
{% set voter_columns = [] %}
{% if execute %}
    {% for column in adapter.get_columns_in_relation(ref("m_people_api__voter")) %}
        {% do voter_columns.append(column.name | lower) %}
    {% endfor %}
{% endif %}

{% set missing = [] %}
{% for minted_type in get_proposed_minted_district_types() %}
    {% if minted_type | lower not in voter_columns %}
        {% do missing.append(minted_type) %}
    {% endif %}
{% endfor %}

{% if missing %}
    select missing_type
    from
    values
        {% for minted_type in missing %}
            ('{{ minted_type }}'){% if not loop.last %},{% endif %}
        {% endfor %}
        as t(missing_type)
{% else %} select cast(null as string) as missing_type where false
{% endif %}
