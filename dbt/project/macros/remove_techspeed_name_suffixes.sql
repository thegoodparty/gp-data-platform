{% macro remove_techspeed_name_suffixes(last_name_column) %}
    trim(
        regexp_replace(
            regexp_replace(
                {{ last_name_column }}, '(?i) (jr\\.?|sr\\.?|ii+|iv|v|i ii)$', ''
            ),
            '(?i) (jr\\.?|sr\\.?|ii+|iv|v|i ii)$',
            ''
        )
    )
{% endmacro %}
