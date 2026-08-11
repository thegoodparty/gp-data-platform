{% test l2_district_tuple_exists(
    model,
    to,
    state_column="state",
    district_type_column="l2_district_type",
    district_name_column="l2_district_name",
    to_state_column="state",
    to_district_type_column="l2_district_type",
    to_district_name_column="l2_district_name",
    row_condition=none
) %}
    {#- Composite relationships test for the (state, district_type, district_name)
        tuple. dbt's built-in relationships test is single-column, and a district
        id is a salted hash of this tuple, so a name drift never dangles an id: it
        nulls a left join, drops an inner join, or mints a second district. -#}
    with
        expected as (
            select distinct
                {{ state_column }} as state,
                {{ district_type_column }} as district_type,
                {{ district_name_column }} as district_name
            from {{ model }}
            where
                {{ district_name_column }} is not null
                {% if row_condition %} and {{ row_condition }} {% endif %}
        ),

        available as (
            select distinct
                {{ to_state_column }} as state,
                {{ to_district_type_column }} as district_type,
                {{ to_district_name_column }} as district_name
            from {{ to }}
        )

    select expected.*
    from expected
    left join
        available
        on expected.state = available.state
        and expected.district_type = available.district_type
        and expected.district_name = available.district_name
    where available.state is null
{% endtest %}
