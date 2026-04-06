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
        -- Attorney (catch all variants: City Attorney, County Attorney, etc.)
        when lower({{ column_name }}) like '%attorney%'
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
        -- Judge (catch all variants: magistrate, appellate, etc.)
        when
            lower({{ column_name }}) like '%judge%'
            or lower({{ column_name }}) like '%magistrate%'
            or lower({{ column_name }}) like '%justice of the peace%'
            or lower({{ column_name }})
            in ('circuit court', 'district court', 'probate court', 'supreme court')
        then 'Judge'
        when lower({{ column_name }}) like '%appellate court%'
        then 'Judge'
        when lower({{ column_name }}) like '%supreme court%'
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
        when lower({{ column_name }}) in ('constable', 'county constable', 'sheriff')
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
        -- Explicitly mapped to Other (not just else fallthrough)
        when
            lower({{ column_name }}) in (
                'board of trustees',
                'community college board',
                'dependent district board',
                'elections supervisor',
                'fire board',
                'hawaiian affairs board',
                'highway superintendent',
                'insurance commissioner',
                'library board',
                'parks and recreation board',
                'port board',
                'township assessor',
                'water supply board'
            )
        then 'Other'
        else 'Other'
    end
{% endmacro %}
