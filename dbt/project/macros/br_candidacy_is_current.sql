{% macro br_candidacy_is_current(election_day, api_database_id, withdrawn, is_hidden) %}
    {#- The API decides whether an upcoming candidacy is still running: it needs a
        current API record that is neither withdrawn nor hidden. Past and
        undated candidacies pass untouched, so history never depends on the API
        having fetched an old id. -#}
    (
        {{ election_day }} < current_date()
        or {{ election_day }} is null
        or (
            {{ api_database_id }} is not null
            and not {{ withdrawn }}
            and not {{ is_hidden }}
        )
    )
{% endmacro %}
