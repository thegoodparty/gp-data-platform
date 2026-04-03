{#
  Maps candidate_office values to standardized office_type categories.
  Shared across all sources (BallotReady, TechSpeed, DDHQ, GP API).

  Each source has its own extraction macro to produce candidate_office:
    - BR: generate_candidate_office_from_position (ballotready_standardizations.sql)
    - DDHQ: parse_ddhq_candidate_office (ddhq_standardizations.sql)
    - TS: pre-populated in upstream model
    - GP API: generate_candidate_office_from_position via campaigns mart

  This macro normalizes those values into a consistent office_type.
  All comparisons use lower() so casing differences (e.g. from initcap
  capitalizing prepositions) don't cause mismatches.
  Usage: {{ map_office_type(column_name) }}
#}
{% macro map_office_type(column_name) %}
    case
        -- Alderman
        when lower({{ column_name }}) in ('alderman', 'alderperson')
        then 'Alderman'
        -- Attorney
        when
            lower({{ column_name }})
            in ('attorney', 'district attorney', 'county attorney')
        then 'Attorney'
        when lower({{ column_name }}) like 'state attorney%'
        then 'Attorney'
        -- City Council
        when
            lower({{ column_name }}) in (
                'city commission',
                'city commissioner',
                'city council',
                'town chair',
                'township council'
            )
        then 'City Council'
        -- Clerk/Treasurer
        when
            lower({{ column_name }}) in (
                'clerk',
                'treasurer',
                'city clerk',
                'township clerk',
                'clerk/treasurer',
                'county clerk',
                'county court clerk'
            )
        then 'Clerk/Treasurer'
        when lower({{ column_name }}) like 'county clerk%'
        then 'Clerk/Treasurer'
        when lower({{ column_name }}) like 'county treasurer%'
        then 'Clerk/Treasurer'
        -- Congressional
        when
            lower({{ column_name }})
            in ('congressional', 'u.s. representative', 'u.s. senator')
        then 'Congressional'
        -- County Supervisor
        when
            lower({{ column_name }}) in (
                'county commissioner',
                'county council',
                'county trustee',
                'county legislature'
            )
        then 'County Supervisor'
        when lower({{ column_name }}) like 'county executive%'
        then 'County Supervisor'
        when lower({{ column_name }}) like 'county legislat%'
        then 'County Supervisor'
        when lower({{ column_name }}) like 'board of supervisor%'
        then 'County Supervisor'
        -- Judge
        when
            lower({{ column_name }}) in (
                'circuit court',
                'circuit court judge',
                'county court judge',
                'district court',
                'judge',
                'probate court',
                'supreme court',
                'trial court judge'
            )
        then 'Judge'
        when lower({{ column_name }}) like 'justice of the peace%'
        then 'Judge'
        when lower({{ column_name }}) like 'state trial court%'
        then 'Judge'
        when lower({{ column_name }}) like 'state appellate court%'
        then 'Judge'
        when lower({{ column_name }}) like 'state supreme court%'
        then 'Judge'
        when lower({{ column_name }}) like 'county court judge%'
        then 'Judge'
        -- Mayor
        when
            lower({{ column_name }}) in ('mayor', 'village president', 'township mayor')
        then 'Mayor'
        -- President
        when lower({{ column_name }}) = 'president'
        then 'President'
        -- School Board
        when lower({{ column_name }}) in ('board of education', 'school board')
        then 'School Board'
        -- Sheriff
        when
            lower({{ column_name }})
            in ('constable', 'county constable', 'county sheriff', 'sheriff')
        then 'Sheriff'
        when lower({{ column_name }}) like 'county sheriff%'
        then 'Sheriff'
        -- State House
        when
            lower({{ column_name }}) in (
                'house of delegates',
                'house of representatives',
                'state assembly',
                'state house',
                'state representative'
            )
        then 'State House'
        -- State Senate
        when lower({{ column_name }}) in ('state senate', 'state senator')
        then 'State Senate'
        -- Statewide/Governor
        when lower({{ column_name }}) in ('governor', 'lieutenant governor')
        then 'Statewide/Governor'
        -- Town Council
        when
            lower({{ column_name }}) in (
                'town council',
                'town trustee',
                'township supervisor',
                'village board',
                'village council',
                'village trustee'
            )
        then 'Town Council'
        else 'Other'
    end
{% endmacro %}
