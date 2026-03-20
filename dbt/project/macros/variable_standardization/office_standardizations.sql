{#
  Maps candidate_office values to standardized office_type categories.
  Shared across all sources (BallotReady, TechSpeed, DDHQ).

  Each source has its own extraction macro to produce candidate_office:
    - BR: generate_candidate_office_from_position (ballotready_standardizations.sql)
    - DDHQ: parse_ddhq_candidate_office (ddhq_standardizations.sql)
    - TS: pre-populated in upstream model

  This macro normalizes those values into a consistent office_type.
  Usage: {{ map_office_type(column_name) }}
#}
{% macro map_office_type(column_name) %}
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
        when {{ column_name }} = 'Town Chair'
        then 'City Council'
        when {{ column_name }} = 'Township Council'
        then 'City Council'
        when {{ column_name }} = 'Clerk'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'Treasurer'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'City Clerk'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'Township Clerk'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'Clerk/Treasurer'
        then 'Clerk/Treasurer'
        when {{ column_name }} = 'Congressional'
        then 'Congressional'
        when {{ column_name }} = 'County Commissioner'
        then 'County Supervisor'
        when {{ column_name }} = 'County Council'
        then 'County Supervisor'
        when {{ column_name }} = 'County Trustee'
        then 'County Supervisor'
        when {{ column_name }} = 'County Legislature'
        then 'County Supervisor'
        when {{ column_name }} = 'Circuit Court'
        then 'Judge'
        when {{ column_name }} = 'Circuit Court Judge'
        then 'Judge'
        when {{ column_name }} = 'County Court Judge'
        then 'Judge'
        when {{ column_name }} = 'District Court'
        then 'Judge'
        when {{ column_name }} = 'Justice of the Peace'
        then 'Judge'
        when {{ column_name }} = 'Probate Court'
        then 'Judge'
        when {{ column_name }} = 'Supreme Court'
        then 'Judge'
        when {{ column_name }} = 'Trial Court Judge'
        then 'Judge'
        when {{ column_name }} = 'Judge'
        then 'Judge'
        when {{ column_name }} = 'Mayor'
        then 'Mayor'
        when {{ column_name }} = 'Village President'
        then 'Mayor'
        when {{ column_name }} = 'President'
        then 'President'
        when {{ column_name }} = 'Board of Education'
        then 'School Board'
        when {{ column_name }} = 'School Board'
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
        -- Explicitly mapped to Other (not just else fallthrough)
        when
            {{ column_name }} in (
                'Board of Trustees',
                'Community College Board',
                'Dependent District Board',
                'Elections Supervisor',
                'Fire Board',
                'Hawaiian Affairs Board',
                'Highway Superintendent',
                'Insurance Commissioner',
                'Library Board',
                'Parks and Recreation Board',
                'Port Board',
                'Township Assessor',
                'Water Supply Board'
            )
        then 'Other'
        else 'Other'
    end
{% endmacro %}
