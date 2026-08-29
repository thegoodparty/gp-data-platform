{% macro br_id_ref_array(json_path) %}
    {#- The id-reference pair appears on six columns across four entities. Declaring the
        struct once keeps them from drifting apart; the enforced contract would catch a
        drift only as a type mismatch pointing at the model, not the typo. -#}
    from_json(
        get_json_object(payload, '{{ json_path }}'),
        'array<struct<databaseId:int,id:string>>'
    )
{% endmacro %}
