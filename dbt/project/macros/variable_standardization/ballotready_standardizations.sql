{#
  BallotReady-specific extraction macros for candidate_office, city, and codes.
  Usage: {{ generate_candidate_code(first_name_col, last_name_col, state_col, office_type_col, city_col=none) }}
  #}
{% macro generate_candidate_code(
    first_name_col, last_name_col, state_col, office_type_col, city_col=none
) %}
    case
        when
            nullif(trim({{ first_name_col }}), '') is null
            or nullif(trim({{ last_name_col }}), '') is null
            or nullif(trim({{ state_col }}), '') is null
            or nullif(trim({{ office_type_col }}), '') is null
            {% if city_col is not none %}
                or nullif(trim({{ city_col }}), '') is null
            {% endif %}
        then null
        else
            lower(
                concat_ws(
                    '__',
                    regexp_replace(
                        regexp_replace(trim({{ first_name_col }}), ' ', '-'),
                        '[^a-zA-Z0-9-]',
                        ''
                    ),
                    regexp_replace(
                        regexp_replace(trim({{ last_name_col }}), ' ', '-'),
                        '[^a-zA-Z0-9-]',
                        ''
                    ),
                    regexp_replace(
                        regexp_replace(trim({{ state_col }}), ' ', '-'),
                        '[^a-zA-Z0-9-]',
                        ''
                    ),
                    {% if city_col is not none %}
                        regexp_replace(
                            regexp_replace(trim({{ city_col }}), ' ', '-'),
                            '[^a-zA-Z0-9-]',
                            ''
                        ),
                    {% endif %}
                    regexp_replace(
                        regexp_replace(trim({{ office_type_col }}), ' ', '-'),
                        '[^a-zA-Z0-9-]',
                        ''
                    )
                )
            )
    end
{% endmacro %}

{#
  This macro extracts city from BallotReady official_office_name
  Usage: {{ extract_city_from_office_name(column_name) }}
  #}
{% macro extract_city_from_office_name(office_name_col) %}
    case
        when {{ office_name_col }} like '% County%'
        then
            left(
                {{ office_name_col }}, position(' County' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% City%'
        then left({{ office_name_col }}, position(' City' in {{ office_name_col }}) - 1)
        when {{ office_name_col }} like '% Village%'
        then
            left(
                {{ office_name_col }}, position(' Village' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Town%'
        then left({{ office_name_col }}, position(' Town' in {{ office_name_col }}) - 1)
        when {{ office_name_col }} like '% Fire%'
        then left({{ office_name_col }}, position(' Fire' in {{ office_name_col }}) - 1)
        when {{ office_name_col }} like '% Public Library%'
        then
            left(
                {{ office_name_col }},
                position(' Public Library' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Library%'
        then
            left(
                {{ office_name_col }}, position(' Library' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Community College%'
        then
            left(
                {{ office_name_col }},
                position(' Community College' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Water District%'
        then
            left(
                {{ office_name_col }},
                position(' Water District' in {{ office_name_col }}) - 1
            )
        when
            position(' District' in {{ office_name_col }}) > 0
            and (
                regexp_extract(trim({{ office_name_col }}), '- District [^ ]+$', 0) = ''
                or position(' District' in {{ office_name_col }})
                < length({{ office_name_col }})
                - length(
                    regexp_extract(trim({{ office_name_col }}), '- District [^ ]+$', 0)
                )
                + 1
            )
        then
            left(
                {{ office_name_col }},
                position(' District' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Independent School%'
        then
            left(
                {{ office_name_col }},
                position(' Independent School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Elementary School%'
        then
            left(
                {{ office_name_col }},
                position(' Elementary School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Area Unified School%'
        then
            left(
                {{ office_name_col }},
                position(' Area Unified School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Area School%'
        then
            left(
                {{ office_name_col }},
                position(' Area School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Area Public School%'
        then
            left(
                {{ office_name_col }},
                position(' Area Public School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Union School%'
        then
            left(
                {{ office_name_col }},
                position(' Union School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% City School%'
        then
            left(
                {{ office_name_col }},
                position(' City School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Unified School%'
        then
            left(
                {{ office_name_col }},
                position(' Unified School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Public School%'
        then
            left(
                {{ office_name_col }},
                position(' Public School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Community School%'
        then
            left(
                {{ office_name_col }},
                position(' Community School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% School%'
        then
            left(
                {{ office_name_col }}, position(' School' in {{ office_name_col }}) - 1
            )
        when {{ office_name_col }} like '% Park District%'
        then
            left(
                {{ office_name_col }},
                position(' Park District' in {{ office_name_col }}) - 1
            )
        else ''
    end
{% endmacro %}


{% macro generate_candidate_office_from_position(
    position_name_col, normalized_position_name_col
) %}
    case
        when {{ position_name_col }} like '%Village President%'
        then 'Village President'
        when {{ position_name_col }} like '%Town Chair%'
        then 'Town Chair'
        when {{ position_name_col }} like '%Water Supply District%'
        then 'Water Supply Board'
        when {{ normalized_position_name_col }} = 'City Legislature'
        then 'City Council'
        when {{ normalized_position_name_col }} = 'Local School Board'
        then 'School Board'
        when {{ normalized_position_name_col }} = 'City Executive//Mayor'
        then 'Mayor'
        when {{ normalized_position_name_col }} = 'Township Treasurer//Fiscal Officer'
        then 'Treasurer'
        when {{ normalized_position_name_col }} = 'Township Trustee//Township Council'
        then 'Township Council'
        when {{ normalized_position_name_col }} = 'Parks and Recreation District Board'
        then 'Parks and Recreation Board'
        when
            {{ normalized_position_name_col }}
            = 'Local Higher Education Board//Community College Board'
        then 'Community College Board'
        when {{ normalized_position_name_col }} = 'City Clerk//Ward Officer'
        then 'City Clerk'
        when {{ normalized_position_name_col }} = 'Library District Board'
        then 'Library Board'
        when {{ normalized_position_name_col }} = 'Fire District Board'
        then 'Fire Board'
        when {{ normalized_position_name_col }} = 'Township Highway Superintendent'
        then 'Highway Superintendent'
        when {{ normalized_position_name_col }} = 'Township Constable'
        then 'Constable'
        when {{ normalized_position_name_col }} = 'City Treasurer//Finance Officer'
        then 'Treasurer'
        when {{ normalized_position_name_col }} = 'State Trial Court Judge - General'
        then 'Trial Court Judge'
        when {{ normalized_position_name_col }} = 'Township Clerk/Treasurer (Joint)'
        then 'Township Clerk'
        when {{ normalized_position_name_col }} = 'Local Court Judge'
        then 'Judge'
        when {{ normalized_position_name_col }} = 'State Senator'
        then 'State Senate'
        when {{ normalized_position_name_col }} = 'City Assessor//Lister'
        then 'City Assessor'
        when {{ normalized_position_name_col }} = 'County Legislature//Executive Board'
        then 'County Legislature'
        when
            {{ normalized_position_name_col }}
            = 'Harbor District Board//Port District Board'
        then 'Port Board'
        else {{ normalized_position_name_col }}
    end
{% endmacro %}
