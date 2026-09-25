-- Each pair is one identifier written two ways; a row comes back when they
-- normalize apart. Numbers are fictional (555 exchange).
{%- set pairs = [
    ("phone", "'(202) 555-0100'", "'+1 (202) 555-0100'"),
    ("phone", "'202-555-0100'", "'1.202.555.0100'"),
    ("phone", "'2025550100'", "'12025550100'"),
    ("email", "'Alice.Example@Example.com '", "'alice.example@example.com'"),
    ("br_person_id", "' 610000 '", "'610000'"),
] %}

with
    normalized as (
        {%- for identifier_type, one_way, another_way in pairs %}
            select
                '{{ identifier_type }}' as identifier_type,
                {{ one_way }} as written_one_way,
                {{ another_way }} as written_another_way,
                {{ dsar_normalize(one_way, identifier_type) }} as normalized_one_way,
                {{ dsar_normalize(another_way, identifier_type) }}
                as normalized_another_way
                {{ "union all" if not loop.last }}
        {%- endfor %}
    )

select *
from normalized
where normalized_one_way != normalized_another_way
