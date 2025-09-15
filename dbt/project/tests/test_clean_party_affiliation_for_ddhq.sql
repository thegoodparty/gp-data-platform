-- Test the clean_party_affiliation_for_ddhq macro with various test cases
-- This analysis file demonstrates the macro functionality
with
    test_data as (
        select *
        from
        values
            -- Basic party names
            ('Democrat', 'Democrat'),
            ('Republican', 'Republican'),
            ('Independent', 'Independent'),
            ('Libertarian', 'Libertarian'),
            ('Green', 'Green'),
            ('Constitution', 'Constitution'),
            ('Reform', 'Reform'),
            ('Working Families', 'Working Families'),
            ('Progressive', 'Progressive'),

            -- Case variations
            ('democrat', 'democrat'),
            ('DEMOCRAT', 'DEMOCRAT'),
            ('republican', 'republican'),
            ('REPUBLICAN', 'REPUBLICAN'),
            ('independent', 'Independent'),
            ('INDEPENDENT', 'Independent'),
            ('libertarian', 'libertarian'),
            ('LIBERTARIAN', 'LIBERTARIAN'),

            -- Nonpartisan standardizations
            ('nonpartisan', 'Nonpartisan'),
            ('NONPARTISAN', 'Nonpartisan'),
            ('Nonpartisan', 'Nonpartisan'),
            ('Nonparisan', 'Nonpartisan'),
            ('Nonpartisian', 'Nonpartisan'),
            ('Non Partisan', 'Nonpartisan'),
            ('non partisan', 'Nonpartisan'),
            ('NON PARTISAN', 'Nonpartisan'),

            -- Libertarian Party standardization
            ('Libertarian Party', 'Libertarian'),
            ('libertarian party', 'libertarian party'),
            ('LIBERTARIAN PARTY', 'LIBERTARIAN PARTY'),

            -- Whitespace handling
            ('  Democrat  ', 'Democrat'),
            ('  Republican  ', 'Republican'),
            ('  Independent  ', 'Independent'),
            ('  nonpartisan  ', 'Nonpartisan'),
            ('  Libertarian Party  ', 'Libertarian'),

            -- Multiple spaces
            ('Democrat    Party', 'Democrat    Party'),
            ('Republican    Party', 'Republican    Party'),
            ('Independent    Candidate', 'Independent    Candidate'),

            -- Null and empty string handling
            ('', null),
            (null, null),
            ('nan', null),
            ('NAN', null),
            ('None', null),
            ('NONE', null),
            ('none', null),

            -- Complex party names
            ('Democratic Party', 'Democratic Party'),
            ('Republican Party', 'Republican Party'),
            ('Green Party', 'Green Party'),
            ('Constitution Party', 'Constitution Party'),
            ('Reform Party', 'Reform Party'),
            ('Working Families Party', 'Working Families Party'),
            ('Progressive Party', 'Progressive Party'),
            ('Socialist Party', 'Socialist Party'),
            ('Communist Party', 'Communist Party'),

            -- Edge cases with special characters
            ('Democrat-Republican', 'Democrat-Republican'),
            ('Independent-Democrat', 'Independent-Democrat'),
            ('Green-Independent', 'Green-Independent'),
            ('Libertarian/Independent', 'Libertarian/Independent'),

            -- Mixed case scenarios
            ('DEMOCRAT party', 'DEMOCRAT party'),
            ('republican PARTY', 'republican PARTY'),
            ('GREEN party', 'GREEN party'),

            -- Long party names
            ('Democratic-Farmer-Labor Party', 'Democratic-Farmer-Labor Party'),
            (
                'Minnesota Democratic-Farmer-Labor Party',
                'Minnesota Democratic-Farmer-Labor Party'
            ),
            ('Constitution Party of America', 'Constitution Party of America'),

            -- Special cases that should remain unchanged
            ('Write-in', 'Write-in'),
            ('Unaffiliated', 'Unaffiliated'),
            ('No Party Preference', 'No Party Preference'),
            ('Decline to State', 'Decline to State'),
            ('Other', 'Other'),
            ('Unknown', 'Unknown') as test_data(
                party_affiliation, expected_cleaned_party_affiliation
            )
    ),
    completed_test_data as (
        select
            party_affiliation as original_party_affiliation,
            expected_cleaned_party_affiliation,
            {{ clean_party_affiliation_for_ddhq("party_affiliation") }}
            as cleaned_party_affiliation
        from test_data
    ),
    invalid_test_data as (
        select *
        from completed_test_data
        where cleaned_party_affiliation != expected_cleaned_party_affiliation
    )

select *
from invalid_test_data
