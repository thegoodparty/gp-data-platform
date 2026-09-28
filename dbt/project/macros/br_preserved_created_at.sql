{% macro br_preserved_created_at() %}
    {#- Pairs with br_current_rows(..., preserve_created_at=true): keep the row's
        existing created_at across an incremental merge, stamping current_timestamp()
        only for a requested_id not seen before. A full refresh has no existing row to
        preserve, so every row stamps fresh. -#}
    {% if is_incremental() %} coalesce(existing_created_at, current_timestamp())
    {%- else %} current_timestamp()
    {%- endif %}
{% endmacro %}
