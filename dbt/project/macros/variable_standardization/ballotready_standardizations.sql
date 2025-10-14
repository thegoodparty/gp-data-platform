{#
  This macro maps Ballotready columns
  Usage: {{ map_office_type(column_name) }}
  #}
{% macro map_ballotready_office_type(column_name) %}
    case
        when {{ column_name }} = 'Alderman'
        then 'Alderman'
        when {{ column_name }} = 'Alderperson'
        then 'Alderman'
        when {{ column_name }} = 'Attorney'
        then 'Attorney'
        when {{ column_name }} = 'City Commission'
        then 'City Council'
        when {{ column_name }} = 'City Commissioner'
        then 'City Council'
        when {{ column_name }} = 'City Council'
        then 'City Council'
        when {{ column_name }} = 'Clerk'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'Treasurer'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'Congressional'
        then 'Congressional'
        when {{ column_name }} = 'County Commissioner'
        then 'County Supervisor'
        when {{ column_name }} = 'County Council'
        then 'County Supervisor'
        when {{ column_name }} = 'County Trustee'
        then 'County Supervisor'
        when {{ column_name }} = 'Circuit Court'
        then 'Judge'
        when {{ column_name }} = 'District Court'
        then 'Judge'
        when {{ column_name }} = 'Justice of the Peace'
        then 'Judge'
        when {{ column_name }} = 'Probate Court'
        then 'Judge'
        when {{ column_name }} = 'Supreme Court'
        then 'Judge'
        when {{ column_name }} = 'Mayor'
        then 'Mayor'
        when {{ column_name }} = 'Village President'
        then 'Mayor'
        when {{ column_name }} = 'Dependent District Board'
        then 'Other'
        when {{ column_name }} = 'Elections Supervisor'
        then 'Other'
        when {{ column_name }} = 'Hawaiian Affairs Board'
        then 'Other'
        when {{ column_name }} = 'Insurance Commissioner'
        then 'Other'
        when {{ column_name }} = 'President'
        then 'President'
        when {{ column_name }} = 'Board of Education'
        then 'School Board'
        when {{ column_name }} = 'Constable'
        then 'Sheriff'
        when {{ column_name }} = 'County Sheriff'
        then 'Sheriff'
        when {{ column_name }} = 'Sheriff'
        then 'Sheriff'
        when {{ column_name }} = 'House of Delegates'
        then 'State House'
        when {{ column_name }} = 'House of Representatives'
        then 'State House'
        when {{ column_name }} = 'State Assembly'
        then 'State House'
        when {{ column_name }} = 'State House'
        then 'State House'
        when {{ column_name }} = 'State Senate'
        then 'State Senate'
        when {{ column_name }} = 'Governor'
        then 'Statewide/Governor'
        when {{ column_name }} = 'Lieutenant Governor'
        then 'Statewide/Governor'
        when {{ column_name }} = 'Town Council'
        then 'Town Council'
        when {{ column_name }} = 'Town Trustee'
        then 'Town Council'
        when {{ column_name }} = 'Township Supervisor'
        then 'Town Council'
        when {{ column_name }} = 'Village Board'
        then 'Town Council'
        when {{ column_name }} = 'Village Council'
        then 'Town Council'
        when {{ column_name }} = 'Village Trustee'
        then 'Town Council'
        when {{ column_name }} = 'City Clerk'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'County Court Judge'
        then 'Judge'
        when {{ column_name }} = 'Circuit Court Judge'
        then 'Judge'
        when {{ column_name }} = 'Trial Court Judge'
        then 'Judge'
        when {{ column_name }} = 'Fire Board'
        then 'Other'
        when {{ column_name }} = 'Library Board'
        then 'Other'
        when {{ column_name }} = 'Judge'
        then 'Judge'
        when {{ column_name }} = 'Community College Board'
        then 'Other'
        when {{ column_name }} = 'School Board'
        then 'School Board'
        when {{ column_name }} = 'Parks and Recreation Board'
        then 'Other'
        when {{ column_name }} = 'Township Assessor'
        then 'Other'
        when {{ column_name }} = 'Town Chair'
        then 'City Council'
        when {{ column_name }} = 'Township Clerk'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'Highway Superintendent'
        then 'Other'
        when {{ column_name }} = 'Township Council'
        then 'City Council'
        when {{ column_name }} = 'Water Supply Board'
        then 'Other'
        else 'Other'
    end
{% endmacro %}

{#
  This macro maps Ballotready columns
  Usage: {{ generate_candidate_code(first_name_col, last_name_col, state_col, office_type_col, city_col=none) }}
  #}
{% macro generate_candidate_code(
    first_name_col, last_name_col, state_col, office_type_col, city_col=none
) %}
    case
        when {{ first_name_col }} is null
        then null
        when {{ last_name_col }} is null
        then null
        when {{ state_col }} is null
        then null
        when {{ office_type_col }} is null
        then null
        {% if city_col is not none %} when {{ city_col }} is null then null {% endif %}
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
                    regexp_replace(
                        regexp_replace(trim({{ office_type_col }}), ' ', '-'),
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
                )
            )
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
