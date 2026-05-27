-- A3: assert the parser produces expected outputs on a fixed fixture
-- AND is idempotent (applying it twice equals applying it once).
with
    fixture(input, expected) as (
        values
            -- Bug shapes to clean
            ('A Bressler', 'Bressler'),
            ('A. Bressler', 'Bressler'),
            ('A.Bressler', 'Bressler'),
            ('Bressler A', 'Bressler'),
            ('A. C. Campbell', 'Campbell'),
            ('A.C. Claytor', 'Claytor'),
            ('E L Zumpano', 'Zumpano'),
            ('M.J. Holbrook', 'Holbrook'),
            -- Already-clean inputs (unchanged)
            ('Smith', 'Smith'),
            ('Bressler', 'Bressler'),
            ('Smith-Jones', 'Smith-Jones'),
            -- Compound surnames (must be preserved)
            ('De La Cruz', 'De La Cruz'),
            ('De Leon', 'De Leon'),
            ('Da Silva', 'Da Silva'),
            ('St. John', 'St. John'),
            ('Mac Donald', 'Mac Donald'),
            ('O''Brien', 'O''Brien'),
            ('AB Smith', 'AB Smith'),
            -- Suffix-bearing (suffix stripped)
            ('Smith Jr.', 'Smith')
    ),

    parser as (
        select
            input,
            expected,
            regexp_replace(
                regexp_replace(
                    {{ remove_name_suffixes("input") }},
                    '^([A-Z][.] ?|[A-Z] )+(?=[A-Za-z])',
                    ''
                ),
                '(?<=[A-Za-z]) [A-Z]$',
                ''
            ) as parsed_once
        from fixture
    ),

    idempotency as (
        select
            input,
            expected,
            parsed_once,
            regexp_replace(
                regexp_replace(
                    {{ remove_name_suffixes("parsed_once") }},
                    '^([A-Z][.] ?|[A-Z] )+(?=[A-Za-z])',
                    ''
                ),
                '(?<=[A-Za-z]) [A-Z]$',
                ''
            ) as parsed_twice
        from parser
    )

select input, expected, parsed_once, parsed_twice
from idempotency
where parsed_once != expected or parsed_once != parsed_twice
