-- Every L2 district type the aggregations unpivot must also be a column on the
-- voter mart. When it is not, that type's districts get a registered_voters
-- count from int__l2_district_aggregations but no DistrictVoter rows at all:
-- a district that looks populated everywhere and serves nobody.
--
-- The voter mart and m_people_api__districtvoter each carry a hand-written
-- district column list while the aggregations drive theirs off
-- get_l2_district_types(). Three lists, no shared source, so they drift. This
-- resolves the mart's real columns at compile time, so it costs no scan.
-- depends_on: {{ ref('m_people_api__voter') }}
{% set voter_columns = [] %}
{% if execute %}
    {% for column in adapter.get_columns_in_relation(ref("m_people_api__voter")) %}
        {% do voter_columns.append(column.name | lower) %}
    {% endfor %}
{% endif %}

{% set missing = [] %}
{% for district_type in get_l2_district_types() %}
    {% if district_type | lower not in voter_columns %}
        {% do missing.append(district_type) %}
    {% endif %}
{% endfor %}

{% if missing %}
    select missing_type
    from
    values
        {% for district_type in missing %}
            ('{{ district_type }}'){% if not loop.last %},{% endif %}
        {% endfor %}
        as t(missing_type)
{% else %} select cast(null as string) as missing_type where false
{% endif %}
