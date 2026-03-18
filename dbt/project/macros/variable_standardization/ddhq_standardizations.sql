{#
  Macros for parsing DDHQ race_name into standardized office fields.
  Mirrors ballotready_standardizations.sql pattern.
#}
{% macro parse_ddhq_candidate_office(race_name_col) %}
    {# Derives candidate_office from DDHQ race_name keywords.
       County branches checked first to prevent %commission board%
       from short-circuiting before %county commission%. #}
    case
        when lower({{ race_name_col }}) like '%mayor%'
        then 'Mayor'
        when lower({{ race_name_col }}) like '%county commission%'
        then 'County Commissioner'
        when
            lower({{ race_name_col }}) like '%county council%'
            or lower({{ race_name_col }}) like '%board of supervisors%'
            or lower({{ race_name_col }}) like '%county board%'
            or lower({{ race_name_col }}) like '%county supervisor%'
        then 'County Legislature'
        when lower({{ race_name_col }}) like '%county treasurer%'
        then 'Clerk/Treasurer'
        when
            lower({{ race_name_col }}) like '%city council%'
            or lower({{ race_name_col }}) like '%city commission%'
            or lower({{ race_name_col }}) like '%common council%'
            or lower({{ race_name_col }}) like '%council member%'
            or lower({{ race_name_col }}) like '%councilmember%'
            or lower({{ race_name_col }}) like '%commission board%'
        then 'City Council'
        when
            lower({{ race_name_col }}) like '%alderperson%'
            or lower({{ race_name_col }}) like '%alderman%'
            or lower({{ race_name_col }}) rlike '\\balder\\b'
        then 'Alderman'
        when
            lower({{ race_name_col }}) like '%town council%'
            or lower({{ race_name_col }}) like '%selectperson%'
        then 'Town Council'
        when
            lower({{ race_name_col }}) like '%town board%'
            or lower({{ race_name_col }}) like '%village board%'
            or lower({{ race_name_col }}) like '%village trustee%'
        then 'Town Council'
        when
            lower({{ race_name_col }}) like '%circuit court%'
            or lower({{ race_name_col }}) like '%municipal judge%'
            or lower({{ race_name_col }}) like '%municipal court%'
        then 'Judge'
        when
            lower({{ race_name_col }}) like '%school board%'
            or lower({{ race_name_col }}) like '%public school%'
            or lower({{ race_name_col }}) like '%school district%'
        then 'School Board'
        when lower({{ race_name_col }}) like '%board of trustees%'
        then 'Board of Trustees'
        when
            lower({{ race_name_col }}) like '%drainage%'
            or lower({{ race_name_col }}) like '%conservation%'
            or lower({{ race_name_col }}) like '%utility district%'
        then 'Other'
    end
{% endmacro %}


{% macro map_ddhq_office_type(race_name_col) %}
    {# Maps DDHQ race_name to standardized office_type categories,
       aligned with map_ballotready_office_type output values. #}
    case
        when lower({{ race_name_col }}) like '%mayor%'
        then 'Mayor'
        when
            lower({{ race_name_col }}) like '%county commission%'
            or lower({{ race_name_col }}) like '%county council%'
            or lower({{ race_name_col }}) like '%board of supervisors%'
            or lower({{ race_name_col }}) like '%county board%'
            or lower({{ race_name_col }}) like '%county supervisor%'
        then 'County Supervisor'
        when lower({{ race_name_col }}) like '%county treasurer%'
        then 'Clerk/Treasurer'
        when
            lower({{ race_name_col }}) like '%city council%'
            or lower({{ race_name_col }}) like '%city commission%'
            or lower({{ race_name_col }}) like '%common council%'
            or lower({{ race_name_col }}) like '%council member%'
            or lower({{ race_name_col }}) like '%councilmember%'
            or lower({{ race_name_col }}) like '%commission board%'
        then 'City Council'
        when
            lower({{ race_name_col }}) like '%alderperson%'
            or lower({{ race_name_col }}) like '%alderman%'
            or lower({{ race_name_col }}) rlike '\\balder\\b'
        then 'Alderman'
        when
            lower({{ race_name_col }}) like '%town council%'
            or lower({{ race_name_col }}) like '%town board%'
            or lower({{ race_name_col }}) like '%village board%'
            or lower({{ race_name_col }}) like '%village trustee%'
            or lower({{ race_name_col }}) like '%selectperson%'
        then 'Town Council'
        when
            lower({{ race_name_col }}) like '%circuit court%'
            or lower({{ race_name_col }}) like '%municipal judge%'
            or lower({{ race_name_col }}) like '%municipal court%'
        then 'Judge'
        when
            lower({{ race_name_col }}) like '%school board%'
            or lower({{ race_name_col }}) like '%public school%'
            or lower({{ race_name_col }}) like '%school district%'
        then 'School Board'
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
        then 'State'
        else 'Local'
    end
{% endmacro %}
