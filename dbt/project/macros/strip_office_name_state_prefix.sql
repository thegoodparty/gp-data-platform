{% macro strip_office_name_state_prefix(race_name_col) %}
    -- Lowercase + trim a source race_name and strip its leading 2-letter state
    -- prefix to produce official_office_name for entity resolution. state is
    -- already a dedicated ExactMatch comparison, so keeping the prefix inside
    -- the office string only inflates Jaro-Winkler on the shared prefix and
    -- guarantees a shared locality token across every race in a state. An empty
    -- result is coerced to NULL.
    nullif(regexp_replace(lower(trim({{ race_name_col }})), '^[a-z]{2} ', ''), '')
{% endmacro %}
