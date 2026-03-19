{% macro remove_name_suffixes(name_column) %}
    trim(
        regexp_replace(
            regexp_replace({{ name_column }}, '(?i) (jr\.?|sr\.?|ii+|iv|v|i ii)$', ''),
            '(?i) (jr\.?|sr\.?|ii+|iv|v|i ii)$',
            ''
        )
    )
{% endmacro %}
