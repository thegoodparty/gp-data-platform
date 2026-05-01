{% macro office_name_tokens(col) %}
    -- Build a normalized array of meaningful tokens from an office-name string,
    -- for use with Splink's ArrayIntersectLevel as a fallback office-overlap
    -- match level (mirrors how first_name_aliases drives nickname matching).
    --
    -- Punctuation is preserved on tokens (matches the existing locality-token
    -- check in scripts/constants.py): "(juneau" stays distinct from "juneau"
    -- so a WI town-of-X (county) ↔ X-county-supervisor pair correctly fails
    -- to overlap, even though both contain a "juneau"-ish substring.
    --
    -- Steps:
    -- 1. Lowercase
    -- 2. Collapse "r- iii" (DDHQ form with internal space) → "r-iii"
    -- 3. Inject "r-N" after "no. N" so DDHQ's MO "Reorganized School
    -- District No. 4" emits an "r-4" token that overlaps with BR's
    -- "Blue Springs R-4 School Board"
    -- 4. Split on whitespace
    -- 5. Per-token CASE: normalize roman-numeral school district codes
    -- (r-i ↔ r-1, r-ii ↔ r-2, …) so DDHQ's "<county> County R-IV" shares
    -- an "r-4" token with BR's "<city> R-4 School Board"
    -- 6. Drop length≤1, pure-digit, and stop-word tokens (stop-word list
    -- mirrors OFFICE_STOP_WORDS in scripts/constants.py)
    filter(
        transform(
            split(
                regexp_replace(
                    regexp_replace(lower({{ col }}), 'r-\\s+', 'r-'),
                    'no\\.\\s+(\\d+)',
                    'no. $1 r-$1'
                ),
                ' '
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
            'no.',
            'odd',
            'unexpired'
        )
    )
{% endmacro %}
