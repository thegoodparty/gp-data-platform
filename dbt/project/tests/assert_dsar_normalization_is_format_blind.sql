-- The register is matched through dsar_normalize on both sides, so a formatting
-- difference between how a number or address was recorded and how a source
-- carries it must never let a row through. Every pair here is the same
-- identifier written two ways; a row comes back when the two normalize apart.
-- The numbers are fictional (555 exchange); no real identifier belongs in this repo.
with
    pairs as (
        select
            stack(
                5,
                'phone',
                '(202) 555-0100',
                '+1 (202) 555-0100',
                'phone',
                '202-555-0100',
                '1.202.555.0100',
                'phone',
                '2025550100',
                '12025550100',
                'email',
                'Alice.Example@Example.com ',
                'alice.example@example.com',
                'br_person_id',
                ' 610000 ',
                '610000'
            ) as (identifier_type, written_one_way, written_another_way)
    ),
    normalized as (
        select
            identifier_type,
            written_one_way,
            written_another_way,
            case
                identifier_type
                when 'phone'
                then {{ dsar_normalize("written_one_way", "phone") }}
                when 'email'
                then {{ dsar_normalize("written_one_way", "email") }}
                else {{ dsar_normalize("written_one_way", "br_person_id") }}
            end as normalized_one_way,
            case
                identifier_type
                when 'phone'
                then {{ dsar_normalize("written_another_way", "phone") }}
                when 'email'
                then {{ dsar_normalize("written_another_way", "email") }}
                else {{ dsar_normalize("written_another_way", "br_person_id") }}
            end as normalized_another_way
        from pairs
    )
select *
from normalized
where normalized_one_way != normalized_another_way
