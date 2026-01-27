-- Generic test to verify that state-level district stats exist
-- This ensures stats are computed for statewide positions (Governor, US Senate, etc.)
{% test state_level_districts_exist(model, district_type) %}

    with
        state_district_stats as (
            select s.district_id, d.type, d.name
            from {{ model }} s
            inner join {{ ref("m_people_api__district") }} d on s.district_id = d.id
            where d.type = '{{ district_type }}'
        ),

        expected_states as (
            select distinct state
            from {{ ref("m_people_api__voter") }}
            where state is not null
        ),

        missing_states as (
            select e.state
            from expected_states e
            left join state_district_stats s on e.state = s.name
            where s.district_id is null
        )

    select *
    from missing_states

{% endtest %}
