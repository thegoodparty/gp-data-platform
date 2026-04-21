{% macro normalize_ddhq_stage_type(column) %}
    {#-
      DDHQ ships a single 'runoff' type without distinguishing primary vs
      general. For pre-2026 data, all DDHQ runoffs follow a general election
      (verified: 387 of 394 DDHQ runoff rows have a general match, 0 have a
      primary match), so we map 'runoff' → 'general runoff'. 'primary runoff'
      only appears in 2026+ BallotReady data, which flags it explicitly.
    -#}
    case when {{ column }} = 'runoff' then 'general runoff' else lower({{ column }}) end
{% endmacro %}
