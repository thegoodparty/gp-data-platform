/*
Reads int__l2_nationwide_uniform_w_haystaq directly (one row per LALVOTERID) and fully rebuilds each
run. L2 loads monthly, so the ~5h full build is tagged "monthly". This previously deduplicated against
an SCD2 snapshot, but the snapshot retained voters removed from L2 (inflating counts) and lagged the
source; reading the current L2 keeps the mart in sync. created_at/updated_at come from the L2 loaded_at.
*/
{{
    config(
        materialized="table",
        auto_liquid_cluster=True,
        tags=["monthly"],
    )
}}

with
    -- Defensive: L2 files for some states have loaded blank fields as '' rather than
    -- null in the
    -- past. Normalize every string column to null once, before the casts below, so it
    -- holds through
    -- them and downstream into green.Voter regardless of how a given state's source
    -- represents blanks.
    source_nulled as (
        select
            {%- for c in adapter.get_columns_in_relation(
                ref("int__l2_nationwide_uniform_w_haystaq")
            ) %}
                {%- if c.is_string() %} nullif(`{{ c.name }}`, '') as `{{ c.name }}`
                {%- else %} `{{ c.name }}`
                {%- endif %}
                {% if not loop.last %},{% endif %}
            {%- endfor %}
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
    ),
    -- (state, district type) pairs where a proposed map is the one legally in force.
    -- Referenced as a semi-join below rather than joined into updated_voters: that
    -- select carries hundreds of unqualified column references, so keeping its FROM
    -- single-table removes any chance of an ambiguity introduced from here.
    adopted_scopes as (
        select concat(state, '|', district_type) as scope
        from {{ ref("district_map_adoption") }}
        where adopted_source = 'proposed' and is_verified
    ),
    updated_voters as (
        select
            -- Core Voter Information
            {{ generate_salted_uuid(fields=["LALVOTERID"], salt="l2") }} as id,
            `LALVOTERID`,
            state_postal_code as `State`,
            `AbsenteeTypes_Description`,
            `Voters_Active` as `Active`,
            cast(`Voters_Age` as string) as `Age`,
            try_cast(`Voters_Age` as int) as `Age_Int`,
            `Voters_FirstName` as `FirstName`,
            `Voters_MiddleName` as `MiddleName`,
            `Voters_LastName` as `LastName`,
            `Voters_Gender` as `Gender`,

            -- Demographics
            `ConsumerData_Business_Owner` as `Business_Owner`,
            -- The data is ISO (yyyy-MM-dd) across all states, but the L2 spec
            -- documents MM/dd/yyyy,
            -- so parse both and keep whichever succeeds (try_to_date returns null,
            -- never errors).
            coalesce(
                try_to_date(`Voters_CalculatedRegDate`, 'yyyy-MM-dd'),
                try_to_date(`Voters_CalculatedRegDate`, 'MM/dd/yyyy')
            ) as `CalculatedRegDate`,
            `CountyEthnic_Description`,
            `CountyEthnic_LALEthnicCode`,
            `Voters_CountyVoterID` as `CountyVoterID`,
            `ConsumerData_Education_Of_Person` as `Education_Of_Person`,
            `ConsumerData_Estimated_Income_Amount` as `Estimated_Income_Amount`,
            cast(
                regexp_replace(
                    `ConsumerData_Estimated_Income_Amount`, '[^0-9.]', ''
                ) as int
            ) as `Estimated_Income_Amount_Int`,
            `EthnicGroups_EthnicGroup1Desc`,
            `Ethnic_Description`,
            `ConsumerData_Homeowner_Probability_Model` as `Homeowner_Probability_Model`,
            `ConsumerData_Language_Code` as `Language_Code`,
            `Mailing_Addresses_AddressLine`,
            `Mailing_Addresses_ApartmentNum`,
            `Mailing_Addresses_ApartmentType`,
            `Mailing_Addresses_CassErrStatCode`,
            try_cast(
                `Mailing_Addresses_CheckDigit` as int
            ) as `Mailing_Addresses_CheckDigit`,
            `Mailing_Addresses_City`,
            `Mailing_Addresses_Designator`,
            `Mailing_Addresses_DPBC`,
            `Mailing_Addresses_ExtraAddressLine`,
            `Mailing_Addresses_HouseNumber`,
            try_cast(
                `Mailing_Addresses_PrefixDirection` as int
            ) as `Mailing_Addresses_PrefixDirection`,
            `Mailing_Addresses_State`,
            `Mailing_Addresses_StreetName`,
            try_cast(
                `Mailing_Addresses_SuffixDirection` as int
            ) as `Mailing_Addresses_SuffixDirection`,
            `Mailing_Addresses_Zip`,
            `Mailing_Addresses_ZipPlus4`,
            `Mailing_Families_FamilyID`,
            -- `Mailing_HHGender_Description`,
            `ConsumerData_Marital_Status` as `Marital_Status`,
            coalesce(
                try_to_date(`Voters_MovedFrom_Date`, 'yyyy-MM-dd'),
                try_to_date(`Voters_MovedFrom_Date`, 'MM/dd/yyyy')
            ) as `MovedFrom_Date`,
            `Voters_MovedFrom_Party_Description` as `MovedFrom_Party_Description`,
            `Voters_MovedFrom_State` as `MovedFrom_State`,
            `Voters_NameSuffix` as `NameSuffix`,
            -- Same date handling as CalculatedRegDate/MovedFrom_Date: raw L2 is a
            -- string, so parse ISO and MM/dd/yyyy and keep whichever succeeds,
            -- otherwise the leading-zero source fix leaves this as a raw string.
            coalesce(
                try_to_date(`Voters_OfficialRegDate`, 'yyyy-MM-dd'),
                try_to_date(`Voters_OfficialRegDate`, 'MM/dd/yyyy')
            ) as `OfficialRegDate`,
            `Parties_Description`,
            `Voters_PlaceOfBirth` as `PlaceOfBirth`,
            `ConsumerData_Presence_Of_Children_in_HH` as `Presence_Of_Children`,
            `Residence_Addresses_AddressLine`,
            `Residence_Addresses_ApartmentNum`,
            `Residence_Addresses_ApartmentType`,
            `Residence_Addresses_CassErrStatCode`,
            try_cast(
                `Residence_Addresses_CheckDigit` as int
            ) as `Residence_Addresses_CheckDigit`,
            `Residence_Addresses_City`,
            `Residence_Addresses_Designator`,
            `Residence_Addresses_DPBC`,
            `Residence_Addresses_ExtraAddressLine`,
            `Residence_Addresses_HouseNumber`,
            `Residence_Addresses_LatLongAccuracy`,
            `Residence_Addresses_Latitude`,
            `Residence_Addresses_Longitude`,
            try_cast(
                `Residence_Addresses_PrefixDirection` as int
            ) as `Residence_Addresses_PrefixDirection`,
            `Residence_Addresses_State`,
            `Residence_Addresses_StreetName`,
            try_cast(
                `Residence_Addresses_SuffixDirection` as int
            ) as `Residence_Addresses_SuffixDirection`,
            `Residence_Addresses_Zip`,
            `Residence_Addresses_ZipPlus4`,
            `Residence_HHParties_Description`,
            `Voters_SequenceOddEven` as `SequenceOddEven`,
            `Voters_SequenceZigZag` as `SequenceZigZag`,
            `Voters_StateVoterID` as `StateVoterID`,
            `ConsumerDataLL_Veteran` as `Veteran_Status`,
            `VoterParties_Change_Changed_Party` as `VoterParties_Change_Changed_Party`,
            -- Possibly add dynamic columns for voter status in later iterations
            -- sum(
            -- (case when {{ '`General_' ~ modules.datetime.datetime.now().strftime('%Y') ~ '`'}} is true then 1 else 0 end)
            -- + (case when {{ '`Primary_' ~ modules.datetime.datetime.now().strftime('%Y') ~ '`'}} is true then 1 else 0 end)
            -- + (case when {{ '`OtherElection_' ~ modules.datetime.datetime.now().strftime('%Y') ~ '`'}} is true then 1 else 0 end)
            -- + (case when {{ '`AnyElection_' ~ modules.datetime.datetime.now().strftime('%Y') ~ '`'}} is true then 1 else 0 end)
            -- ) as `Voter_Status`,
            current_timestamp() as `Voter_Status_UpdatedAt`,
            cast(
                `VoterTelephones_CellConfidenceCode` as int
            ) as `VoterTelephones_CellConfidenceCode`,
            `VoterTelephones_CellPhoneFormatted`,
            cast(
                `VoterTelephones_LandlineConfidenceCode` as int
            ) as `VoterTelephones_LandlineConfidenceCode`,
            `VoterTelephones_LandlineFormatted`,

            -- Voter Turnout
            `AnyElection_2017`,
            `AnyElection_2019`,
            `AnyElection_2021`,
            `AnyElection_2023`,
            `AnyElection_2025`,
            `General_2016`,
            `General_2018`,
            `General_2020`,
            `General_2022`,
            `General_2024`,
            `General_2026`,
            `OtherElection_2016`,
            `OtherElection_2018`,
            `OtherElection_2020`,
            `OtherElection_2022`,
            `OtherElection_2024`,
            `OtherElection_2026`,
            `PresidentialPrimary_2016`,
            `PresidentialPrimary_2020`,
            `PresidentialPrimary_2024`,
            `Primary_2016`,
            `Primary_2018`,
            `Primary_2020`,
            `Primary_2022`,
            `Primary_2024`,
            `Primary_2026`,
            -- The int model casts these to double for its other consumers, but prod
            -- stores them as
            -- integer text ('42', not '42.0'); round-trip through int so the serving
            -- text matches.
            cast(
                cast(`Voters_VotingPerformanceEvenYearGeneral` as int) as string
            ) as `VotingPerformanceEvenYearGeneral`,
            cast(
                cast(
                    `Voters_VotingPerformanceEvenYearGeneralAndPrimary` as int
                ) as string
            ) as `VotingPerformanceEvenYearGeneralAndPrimary`,
            cast(
                cast(`Voters_VotingPerformanceEvenYearPrimary` as int) as string
            ) as `VotingPerformanceEvenYearPrimary`,
            cast(
                cast(`Voters_VotingPerformanceMinorElection` as int) as string
            ) as `VotingPerformanceMinorElection`,

            -- Districts
            {{ get_l2_district_columns() }},
            -- District-adjacent columns outside get_l2_district_columns,
            -- kept for the people-api Voter contract.
            `AddressDistricts_Change_Changed_CD`,
            `AddressDistricts_Change_Changed_County`,
            `AddressDistricts_Change_Changed_HD`,
            `AddressDistricts_Change_Changed_LD`,
            `AddressDistricts_Change_Changed_SD`,
            `Democratic_Convention_Member`,
            `Democratic_Zone`,
            `Voters_FIPS` as `FIPS`,
            `Precinct`,
            `Proposed_City`,
            `Proposed_City_Commissioner_District`,
            `Proposed_Community_College`,
            `Proposed_District`,
            -- The adopted map, split out of the vendor's catch-all Proposed_District
            -- into one column per handled type and named to match the minted district
            -- type. m_people_api__districtvoter derives the columns it unpivots by
            -- intersecting voter columns with district types, so naming them this way
            -- is what makes DistrictVoter rows appear for these districts with no list
            -- to maintain there.
            --
            -- Null unless the adoption seed records that state's proposed map as the
            -- one legally in force, so a struck-down map (Virginia) never reaches a
            -- voter row. The seed's per-district narrowing is not applied here; every
            -- row is state-level today, and a narrowed row would simply not mint the
            -- district, so no DistrictVoter row could form regardless.
            case
                when
                    {{ proposed_district_type("`Proposed_District`") }}
                    = 'US_Congressional_District'
                    and concat(state_postal_code, '|US_Congressional_District')
                    in (select scope from adopted_scopes)
                then {{ proposed_district_number("`Proposed_District`") }}
            end as `Congressional_District_2026`,
            case
                when
                    {{ proposed_district_type("`Proposed_District`") }}
                    = 'State_Senate_District'
                    and concat(state_postal_code, '|State_Senate_District')
                    in (select scope from adopted_scopes)
                then {{ proposed_district_number("`Proposed_District`") }}
            end as `State_Senate_District_2026`,
            `Proposed_Elementary_School_District`,
            `Proposed_Fire_District`,
            `Proposed_Unified_School_District`,
            `Republican_Area`,
            `Republican_Convention_Member`,
            `Vote_By_Mail_Area`,
            `hf_most_important_policy_item`,
            loaded_at
        from source_nulled
    ),
    voter_propensity as (
        select `LALVOTERID`, `prob_vote`
        from {{ ref("stg_model_predictions__voter_turnout_scores_20260730") }}
    ),
    /*
        Note that here we need to list each column individually since we need to
        explicitly case protect each column name with backticks to match the Voter table
        in the people-api schema.
    */
    final as (
        select
            -- Core Voter Information
            tbl_updated.id,
            tbl_updated.`LALVOTERID`,
            tbl_updated.`State`,
            tbl_updated.`AbsenteeTypes_Description`,
            tbl_updated.`Active`,
            tbl_updated.`Age`,
            tbl_updated.`Age_Int`,
            tbl_updated.`FirstName`,
            tbl_updated.`MiddleName`,
            tbl_updated.`LastName`,
            tbl_updated.`Gender`,

            -- Demographics
            tbl_updated.`Business_Owner`,
            tbl_updated.`CalculatedRegDate`,
            tbl_updated.`CountyEthnic_Description`,
            tbl_updated.`CountyEthnic_LALEthnicCode`,
            tbl_updated.`CountyVoterID`,
            tbl_updated.`Education_Of_Person`,
            tbl_updated.`Estimated_Income_Amount`,
            tbl_updated.`Estimated_Income_Amount_Int`,
            tbl_updated.`EthnicGroups_EthnicGroup1Desc`,
            tbl_updated.`Ethnic_Description`,
            tbl_updated.`Homeowner_Probability_Model`,
            tbl_updated.`Language_Code`,
            tbl_updated.`Mailing_Addresses_AddressLine`,
            tbl_updated.`Mailing_Addresses_ApartmentNum`,
            tbl_updated.`Mailing_Addresses_ApartmentType`,
            tbl_updated.`Mailing_Addresses_CassErrStatCode`,
            tbl_updated.`Mailing_Addresses_CheckDigit`,
            tbl_updated.`Mailing_Addresses_City`,
            tbl_updated.`Mailing_Addresses_Designator`,
            tbl_updated.`Mailing_Addresses_DPBC`,
            tbl_updated.`Mailing_Addresses_ExtraAddressLine`,
            tbl_updated.`Mailing_Addresses_HouseNumber`,
            tbl_updated.`Mailing_Addresses_PrefixDirection`,
            tbl_updated.`Mailing_Addresses_State`,
            tbl_updated.`Mailing_Addresses_StreetName`,
            tbl_updated.`Mailing_Addresses_SuffixDirection`,
            tbl_updated.`Mailing_Addresses_Zip`,
            tbl_updated.`Mailing_Addresses_ZipPlus4`,
            tbl_updated.`Mailing_Families_FamilyID`,
            -- tbl_updated.`Mailing_HHGender_Description`,
            tbl_updated.`Marital_Status`,
            tbl_updated.`MovedFrom_Date`,
            tbl_updated.`MovedFrom_Party_Description`,
            tbl_updated.`MovedFrom_State`,
            tbl_updated.`NameSuffix`,
            tbl_updated.`OfficialRegDate`,
            tbl_updated.`Parties_Description`,
            tbl_updated.`PlaceOfBirth`,
            tbl_updated.`Presence_Of_Children`,
            tbl_updated.`Residence_Addresses_AddressLine`,
            tbl_updated.`Residence_Addresses_ApartmentNum`,
            tbl_updated.`Residence_Addresses_ApartmentType`,
            tbl_updated.`Residence_Addresses_CassErrStatCode`,
            tbl_updated.`Residence_Addresses_CheckDigit`,
            tbl_updated.`Residence_Addresses_City`,
            tbl_updated.`Residence_Addresses_Designator`,
            tbl_updated.`Residence_Addresses_DPBC`,
            tbl_updated.`Residence_Addresses_ExtraAddressLine`,
            tbl_updated.`Residence_Addresses_HouseNumber`,
            tbl_updated.`Residence_Addresses_LatLongAccuracy`,
            tbl_updated.`Residence_Addresses_Latitude`,
            tbl_updated.`Residence_Addresses_Longitude`,
            tbl_updated.`Residence_Addresses_PrefixDirection`,
            tbl_updated.`Residence_Addresses_State`,
            tbl_updated.`Residence_Addresses_StreetName`,
            tbl_updated.`Residence_Addresses_SuffixDirection`,
            tbl_updated.`Residence_Addresses_Zip`,
            tbl_updated.`Residence_Addresses_ZipPlus4`,
            tbl_updated.`Residence_HHParties_Description`,
            tbl_updated.`SequenceOddEven`,
            tbl_updated.`SequenceZigZag`,
            tbl_updated.`StateVoterID`,
            tbl_updated.`Veteran_Status`,
            tbl_updated.`VoterParties_Change_Changed_Party`,
            case
                when tbl_propensity.`prob_vote` is null
                then 'Unknown'
                when tbl_propensity.`prob_vote` < 0.25
                then 'Unlikely'
                when tbl_propensity.`prob_vote` < 0.50
                then 'Unreliable'
                when tbl_propensity.`prob_vote` < 0.75
                then 'Likely'
                else 'Super'
            end as `Voter_Status`,
            tbl_propensity.`prob_vote` as `Voter_Turnout_Probability`,
            tbl_updated.`Voter_Status_UpdatedAt`,
            tbl_updated.`VoterTelephones_CellConfidenceCode`,
            tbl_updated.`VoterTelephones_CellPhoneFormatted`,
            tbl_updated.`VoterTelephones_LandlineConfidenceCode`,
            tbl_updated.`VoterTelephones_LandlineFormatted`,

            -- Voter Turnout
            tbl_updated.`AnyElection_2017`,
            tbl_updated.`AnyElection_2019`,
            tbl_updated.`AnyElection_2021`,
            tbl_updated.`AnyElection_2023`,
            tbl_updated.`AnyElection_2025`,
            tbl_updated.`General_2016`,
            tbl_updated.`General_2018`,
            tbl_updated.`General_2020`,
            tbl_updated.`General_2022`,
            tbl_updated.`General_2024`,
            tbl_updated.`General_2026`,
            tbl_updated.`OtherElection_2016`,
            tbl_updated.`OtherElection_2018`,
            tbl_updated.`OtherElection_2020`,
            tbl_updated.`OtherElection_2022`,
            tbl_updated.`OtherElection_2024`,
            tbl_updated.`OtherElection_2026`,
            tbl_updated.`PresidentialPrimary_2016`,
            tbl_updated.`PresidentialPrimary_2020`,
            tbl_updated.`PresidentialPrimary_2024`,
            tbl_updated.`Primary_2016`,
            tbl_updated.`Primary_2018`,
            tbl_updated.`Primary_2020`,
            tbl_updated.`Primary_2022`,
            tbl_updated.`Primary_2024`,
            tbl_updated.`Primary_2026`,
            tbl_updated.`VotingPerformanceEvenYearGeneral`,
            tbl_updated.`VotingPerformanceEvenYearGeneralAndPrimary`,
            tbl_updated.`VotingPerformanceEvenYearPrimary`,
            tbl_updated.`VotingPerformanceMinorElection`,

            -- Districts
            {{ get_l2_district_columns(table_alias="tbl_updated") }},
            -- District-adjacent columns outside get_l2_district_columns,
            -- kept for the people-api Voter contract.
            tbl_updated.`AddressDistricts_Change_Changed_CD`,
            tbl_updated.`AddressDistricts_Change_Changed_County`,
            tbl_updated.`AddressDistricts_Change_Changed_HD`,
            tbl_updated.`AddressDistricts_Change_Changed_LD`,
            tbl_updated.`AddressDistricts_Change_Changed_SD`,
            tbl_updated.`Democratic_Convention_Member`,
            tbl_updated.`Democratic_Zone`,
            tbl_updated.`FIPS`,
            tbl_updated.`Precinct`,
            tbl_updated.`Proposed_City`,
            tbl_updated.`Proposed_City_Commissioner_District`,
            tbl_updated.`Proposed_Community_College`,
            tbl_updated.`Proposed_District`,
            tbl_updated.`Congressional_District_2026`,
            tbl_updated.`State_Senate_District_2026`,
            tbl_updated.`Proposed_Elementary_School_District`,
            tbl_updated.`Proposed_Fire_District`,
            tbl_updated.`Proposed_Unified_School_District`,
            tbl_updated.`Republican_Area`,
            tbl_updated.`Republican_Convention_Member`,
            tbl_updated.`Vote_By_Mail_Area`,
            tbl_updated.`hf_most_important_policy_item`,
            -- No first-seen history without the snapshot: both timestamps are the L2
            -- load time.
            tbl_updated.`loaded_at` as created_at,
            tbl_updated.`loaded_at` as updated_at
        from updated_voters as tbl_updated
        left join
            voter_propensity as tbl_propensity
            on tbl_updated.`LALVOTERID` = tbl_propensity.`LALVOTERID`
    )

select *
from final
