-- Test the office_match_keys macro (drives DDHQ supplement matching in
-- m_election_api__elected_official_support).
-- Passes when all rows return expected results (query returns 0 rows).
with
    test_data as (
        select *
        from
        values
            -- (name, exp_locality, exp_category, exp_seat)
            -- Cross-source name variants collapse to the same locality_key
            ('City of Aurora', 'aurora', 'other', cast(null as string)),
            ('Aurora City', 'aurora', 'other', cast(null as string)),
            ('Lincoln Township', 'lincoln', 'other', cast(null as string)),
            ('Lincoln Twp', 'lincoln', 'other', cast(null as string)),
            -- Councilor / Councillor / Council all classify as council
            ('Beverly Councilor At Large', 'beverly', 'council', 'atlarge'),
            ('Beverly City Council At-Large', 'beverly', 'council', 'atlarge'),
            -- Numbered seat beats the at-large fallback. The seat number is a
            -- pure-digit token, which the shared tokenizer drops from locality;
            -- seat_designator carries it.
            ('Acworth City Council At-large Post 4', 'acworth', 'council', '4'),
            -- Keyword + number, and trailing-number fallback
            ('Hondo City Council Ward 3', 'hondo', 'council', '3'),
            ('Topeka City Council 7', 'topeka', 'council', '7'),
            -- Village/town board -> legislative (council) category
            (
                'Carpentersville Village Board',
                'carpentersville',
                'council',
                cast(null as string)
            ),
            -- Category classification
            ('Plainfield School Board', 'plainfield', 'school', cast(null as string)),
            ('Springfield Mayor', 'springfield', 'mayor', cast(null as string)),
            -- All-stopword name -> empty locality_key
            ('City Council', '', 'council', cast(null as string)),
            -- Digit-leading token is the only distinguishing content and must
            -- be kept (number precedes keyword, so seat_designator is null)
            ('42nd Ward', '42nd', 'other', cast(null as string)) as t(
                name, exp_locality, exp_category, exp_seat
            )
    ),

    results as (
        select
            name, exp_locality, exp_category, exp_seat, {{ office_match_keys("name") }}
        from test_data
    )

select *
from results
where
    array_join(locality_key, ' ') != exp_locality
    or office_category != exp_category
    or not (seat_designator <=> exp_seat)
