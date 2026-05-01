{% macro office_name_tokens(col) %}
    -- Normalized token array for Splink ArrayIntersectLevel office-overlap.
    --
    -- Trailing punctuation is stripped so "county," matches the "county" stop
    -- word and "r-iv," normalizes to "r-4". Leading punctuation is preserved:
    -- "(juneau" (parenthetical-county marker on a WI town-of-X row) must stay
    -- distinct from "juneau" (substantive locality on a county-supervisor row)
    -- to avoid a false-positive office overlap.
    --
    -- Roman-numeral and "No. N" → "r-N" rewrites let DDHQ "Lincoln County
    -- R-IV School District" intersect with BR "Winfield R-4 School Board".
    --
    -- Stop-word list mirrors OFFICE_STOP_WORDS in the matcha repo's
    -- scripts/constants.py — keep both in sync when editing.
    filter(
        transform(
            transform(
                split(
                    regexp_replace(
                        regexp_replace(lower({{ col }}), 'r-\\s+', 'r-'),
                        'no\\.\\s+(\\d+)',
                        'no. $1 r-$1'
                    ),
                    ' '
                ),
                t -> regexp_replace(t, '\\p{Punct}+$', '')
            ),
            t -> case
                when t = 'r-i'
                then 'r-1'
                when t = 'r-ii'
                then 'r-2'
                when t = 'r-iii'
                then 'r-3'
                when t = 'r-iv'
                then 'r-4'
                when t = 'r-v'
                then 'r-5'
                when t = 'r-vi'
                then 'r-6'
                when t = 'r-vii'
                then 'r-7'
                when t = 'r-viii'
                then 'r-8'
                when t = 'r-ix'
                then 'r-9'
                when t = 'r-x'
                then 'r-10'
                else t
            end
        ),
        t -> length(t) > 1
        and not regexp_like(t, '^[0-9]+$')
        and t not in (
            'city',
            'of',
            'the',
            'county',
            'board',
            'council',
            'school',
            'district',
            'mayor',
            'alderperson',
            'trustee',
            'at',
            'large',
            'zone',
            'ward',
            'seat',
            'position',
            'commission',
            'precinct',
            'town',
            'village',
            'member',
            'councilmember',
            'supervisor',
            'supervisors',
            'commissioner',
            'judge',
            'branch',
            'education',
            'unified',
            'public',
            'elementary',
            'consolidated',
            'central',
            'special',
            'independent',
            'office',
            'clerk',
            'treasurer',
            'coroner',
            'sheriff',
            'magistrate',
            'property',
            'value',
            'administrator',
            'emergency',
            'services',
            'director',
            'justice',
            'peace',
            'representative',
            'house',
            'representatives',
            'legislature',
            'legislative',
            'metro',
            'and',
            'for',
            'no',
            'odd',
            'unexpired'
        )
    )
{% endmacro %}
