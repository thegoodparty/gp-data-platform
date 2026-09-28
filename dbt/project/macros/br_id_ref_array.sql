{% macro br_id_ref_array(json_path) %}
    {#- The id-reference pair appears on six columns across four entities. Declaring the
        struct once keeps them from drifting apart; the enforced contract would catch a
        drift only as a type mismatch pointing at the model, not the typo. -#}
    {#- Empty rather than null when the key is absent: the Python models normalised a
        missing list to [], prod holds no null in any of these columns, and the
        not_null tests on them would fail on a null. -#}
    coalesce(
        from_json(
            get_json_object(payload, '{{ json_path }}'),
            'array<struct<databaseId:int,id:string>>'
        ),
        from_json('[]', 'array<struct<databaseId:int,id:string>>')
    )
{% endmacro %}
