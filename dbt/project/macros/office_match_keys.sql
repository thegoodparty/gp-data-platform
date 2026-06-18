{#
  Emits three comparable keys for fuzzy office-name matching across sources
  (e.g. DDHQ race name vs BallotReady position name). Use in a SELECT:
      {{ office_match_keys('pos.name') }}
  produces: locality_key (sorted array of distinctive place tokens),
  office_category (council/school/commission/...), seat_designator
  (atlarge | a seat/ward/district/post number | null).

  Two offices are the same when state + locality_key + office_category match and
  seat_designator is null-safe equal. This captures the variant families an
  LLM cross-reference surfaced (City of X / X City, Twp / Township,
  Councilor / Council, numbered seats, at-large) while keeping different
  localities (Logan vs North Logan) and office types (council vs school board)
  apart.
#}
{% macro office_match_keys(col) %}
    array_sort(
        filter(
            array_except(
                split(regexp_replace(lower(trim({{ col }})), '[^a-z0-9]+', ' '), ' '),
                array(
                    'city',
                    'town',
                    'township',
                    'twp',
                    'county',
                    'council',
                    'councilor',
                    'councillor',
                    'alderman',
                    'selectman',
                    'board',
                    'education',
                    'school',
                    'district',
                    'commission',
                    'commissioner',
                    'trustee',
                    'trustees',
                    'mayor',
                    'clerk',
                    'treasurer',
                    'supervisor',
                    'member',
                    'members',
                    'at',
                    'large',
                    'seat',
                    'ward',
                    'post',
                    'place',
                    'group',
                    'division',
                    'precinct',
                    'of',
                    'the',
                    'village',
                    'borough',
                    'local',
                    'high',
                    'area',
                    'unified',
                    'community',
                    'consolidated',
                    'regional',
                    'position',
                    'and',
                    'for',
                    ''
                )
            ),
            x -> x rlike '^[a-z]'
        )
    ) as locality_key,
    case
        when lower({{ col }}) rlike 'school|education'
        then 'school'
        when lower({{ col }}) rlike 'council|councillor|councilor|alderman|selectman'
        then 'council'
        when lower({{ col }}) rlike 'commission'
        then 'commission'
        when lower({{ col }}) rlike 'trustee'
        then 'trustee'
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
        when lower({{ col }}) rlike 'at.?large'
        then 'atlarge'
        when
            regexp_extract(
                lower({{ col }}),
                '(ward|district|post|seat|place|group|division|precinct) *#? *([0-9]+)',
                2
            )
            <> ''
        then
            regexp_extract(
                lower({{ col }}),
                '(ward|district|post|seat|place|group|division|precinct) *#? *([0-9]+)',
                2
            )
        when regexp_extract(lower({{ col }}), '([0-9]+) *$', 1) <> ''
        then regexp_extract(lower({{ col }}), '([0-9]+) *$', 1)
    end as seat_designator
{% endmacro %}
