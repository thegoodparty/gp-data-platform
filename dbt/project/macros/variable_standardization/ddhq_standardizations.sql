{#
  Macros for parsing DDHQ race_name into standardized office fields.
  office_type is derived by feeding parse_ddhq_candidate_office output
  through the shared map_office_type macro (in office_standardizations.sql).
#}
{% macro parse_ddhq_candidate_office(race_name_col) %}
    {# Derives candidate_office from DDHQ race_name keywords.
       Order matters: more-specific patterns (county sheriff, county clerk) must
       precede broad county/city catch-alls so they aren't swallowed. #}
    case
        -- Mayor
        when lower({{ race_name_col }}) like '%mayor%'
        then 'Mayor'
        when lower({{ race_name_col }}) like '%village president%'
        then 'Village President'

        -- County-level offices (specific before general)
        when lower({{ race_name_col }}) like '%county sheriff%'
        then 'County Sheriff'
        when lower({{ race_name_col }}) like '%county clerk%'
        then 'Clerk'
        when lower({{ race_name_col }}) like '%county treasurer%'
        then 'Treasurer'
        when lower({{ race_name_col }}) like '%county commission%'
        then 'County Commissioner'
        when
            lower({{ race_name_col }}) like '%county council%'
            or lower({{ race_name_col }}) like '%board of supervisors%'
            or lower({{ race_name_col }}) like '%county board%'
            or lower({{ race_name_col }}) like '%county supervisor%'
        then 'County Legislature'
        when lower({{ race_name_col }}) like '%county court%'
        then 'Judge'
        when lower({{ race_name_col }}) like '%county constable%'
        then 'Constable'

        -- City-level legislative bodies
        when
            lower({{ race_name_col }}) like '%city council%'
            or lower({{ race_name_col }}) like '%city commission%'
            or lower({{ race_name_col }}) like '%common council%'
            or lower({{ race_name_col }}) like '%council member%'
            or lower({{ race_name_col }}) like '%councilmember%'
            or lower({{ race_name_col }}) like '%councilor%'
            or lower({{ race_name_col }}) like '%councillor%'
            or lower({{ race_name_col }}) like '%commission board%'
            or lower({{ race_name_col }}) like '%borough council%'
        then 'City Council'

        -- Alderman
        when
            lower({{ race_name_col }}) like '%alderperson%'
            or lower({{ race_name_col }}) like '%alderman%'
            or lower({{ race_name_col }}) rlike '\\balder\\b'
        then 'Alderman'

        -- Town/village-level legislative bodies
        when
            lower({{ race_name_col }}) like '%town council%'
            or lower({{ race_name_col }}) like '%town board%'
            or lower({{ race_name_col }}) like '%village board%'
            or lower({{ race_name_col }}) like '%village trustee%'
            or lower({{ race_name_col }}) like '%village council%'
            or lower({{ race_name_col }}) like '%selectperson%'
            or lower({{ race_name_col }}) like '%selectman%'
            or lower({{ race_name_col }}) like '%selectmen%'
            or lower({{ race_name_col }}) like '%select board%'
        then 'Town Council'

        -- Township offices
        when lower({{ race_name_col }}) like '%township trustee%'
        then 'Township Supervisor'
        when lower({{ race_name_col }}) like '%township clerk%'
        then 'Township Clerk'

        -- Judicial
        when
            lower({{ race_name_col }}) like '%circuit court%'
            or lower({{ race_name_col }}) like '%municipal judge%'
            or lower({{ race_name_col }}) like '%municipal court%'
            or lower({{ race_name_col }}) like '%justice of the peace%'
            or lower({{ race_name_col }}) like '%town justice%'
        then 'Judge'

        -- Education
        when
            lower({{ race_name_col }}) like '%school board%'
            or lower({{ race_name_col }}) like '%public school%'
            or lower({{ race_name_col }}) like '%school district%'
            or lower({{ race_name_col }}) like '%school committee%'
            or lower({{ race_name_col }}) like '%board of education%'
        then 'School Board'
        when lower({{ race_name_col }}) like '%board of trustees%'
        then 'Board of Trustees'

        -- Clerk/Treasurer (general, after county-specific)
        when
            lower({{ race_name_col }}) like '%town clerk%'
            or lower({{ race_name_col }}) like '%city clerk%'
        then 'Clerk'
        when lower({{ race_name_col }}) like '%treasurer%'
        then 'Treasurer'

        -- Constable / Sheriff (general, after county-specific)
        when lower({{ race_name_col }}) like '%constable%'
        then 'Constable'
        when lower({{ race_name_col }}) like '%sheriff%'
        then 'County Sheriff'

        -- Assembly / representative bodies
        when lower({{ race_name_col }}) like '%representative town meeting%'
        then 'City Council'

        -- Special districts and other offices
        when
            lower({{ race_name_col }}) like '%library board%'
            or lower({{ race_name_col }}) like '%library district%'
            or lower({{ race_name_col }}) like '%library trustee%'
        then 'Library Board'
        when
            lower({{ race_name_col }}) like '%fire district%'
            or lower({{ race_name_col }}) like '%fire board%'
            or lower({{ race_name_col }}) like '%fire commission%'
            or lower({{ race_name_col }}) like '%fire department%'
        then 'Fire Board'
        when
            lower({{ race_name_col }}) like '%park district%'
            or lower({{ race_name_col }}) like '%park board%'
            or lower({{ race_name_col }}) like '%parks and recreation%'
            or lower({{ race_name_col }}) like '%recreation district%'
            or lower({{ race_name_col }}) like '%recreation board%'
        then 'Parks and Recreation Board'
        when
            lower({{ race_name_col }}) like '%water district%'
            or lower({{ race_name_col }}) like '%water board%'
            or lower({{ race_name_col }}) like '%water authority%'
            or lower({{ race_name_col }}) like '%sewer district%'
            or lower({{ race_name_col }}) like '%sewer board%'
            or lower({{ race_name_col }}) like '%water supply%'
        then 'Water Supply Board'
        when
            lower({{ race_name_col }}) like '%drainage%'
            or lower({{ race_name_col }}) like '%conservation%'
            or lower({{ race_name_col }}) like '%utility district%'
        then 'Other'
        else 'Other'
    end
{% endmacro %}


{% macro parse_ddhq_office_level(race_name_col) %}
    {# Derives office_level from DDHQ race_name. #}
    case
        when lower({{ race_name_col }}) like '%county%'
        then 'County'
        when
            lower({{ race_name_col }}) like '%state senate%'
            or lower({{ race_name_col }}) like '%state house%'
            or lower({{ race_name_col }}) like '%state assembly%'
            or lower({{ race_name_col }}) like '%state circuit court%'
            or lower({{ race_name_col }}) like '%court of appeals%'
        then 'State'
        else 'Local'
    end
{% endmacro %}
