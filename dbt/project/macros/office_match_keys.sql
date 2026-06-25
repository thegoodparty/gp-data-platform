{#
  Emits three comparable keys for fuzzy office-name matching across sources
  (e.g. DDHQ race name vs BallotReady position name). Use in a SELECT:
      {{ office_match_keys('pos.name') }}
  produces: locality_key (sorted array of distinctive place tokens),
  office_category (school/council/mayor/...), seat_designator
  (a seat/ward/district/post/zone number | atlarge | null).

  Two offices are the same when state + locality_key + office_category match and
  seat_designator is null-safe equal (an unnumbered office is also compatible with
  a DDHQ at-large race). This captures the variant families an LLM cross-reference
  surfaced (City of X / X City, Twp / Township, Councilor / Council, Council /
  Commission / Trustee board, numbered seats, at-large, descriptor words like
  Municipal / Public / Committee) while keeping different localities (Logan vs
  North Logan) and office types (council vs school board) apart.
#}
{% macro office_match_keys(col) %}
    -- locality_key: distinctive place tokens, sorted. Reuses office_name_tokens
    -- as the single tokenizer/stop-word list (shared with the candidacy and
    -- elected_official matchers), rather than maintaining a second copy here.
    array_sort({{ office_name_tokens(col) }}) as locality_key,
    case
        when lower({{ col }}) rlike 'school|education'
        then 'school'
        when
            lower({{ col }})
            rlike 'council|councillor|councilor|alderman|selectman|commission|trustee'
        then 'council'
        when
            lower({{ col }}) rlike '\\b(village|town|township|borough)\\b'
            and lower({{ col }}) rlike '\\bboard\\b'
        then 'council'
        when lower({{ col }}) rlike 'mayor'
        then 'mayor'
        when lower({{ col }}) rlike 'clerk'
        then 'clerk'
        when lower({{ col }}) rlike 'treasurer'
        then 'treasurer'
        when lower({{ col }}) rlike 'supervisor'
        then 'supervisor'
        when lower({{ col }}) rlike 'judge'
        then 'judge'
        else 'other'
    end as office_category,
    case
        -- a numbered seat is more specific than "at-large", so check it first
        when
            regexp_extract(
                lower({{ col }}),
                '(ward|seat|post|place|group|division|precinct|zone|district) *#? *([0-9]+)',
                2
            )
            <> ''
        then
            regexp_extract(
                lower({{ col }}),
                '(ward|seat|post|place|group|division|precinct|zone|district) *#? *([0-9]+)',
                2
            )
        when lower({{ col }}) rlike 'at.?large'
        then 'atlarge'
        when regexp_extract(lower({{ col }}), '([0-9]+) *$', 1) <> ''
        then regexp_extract(lower({{ col }}), '([0-9]+) *$', 1)
    end as seat_designator
{% endmacro %}
