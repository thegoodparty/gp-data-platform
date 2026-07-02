/*
Deduplication for this voter data is done by comparing each row against a snapshot. With the current size of
220 MM rows/voters and ~350 columns, this processes takes nearly 5 hours. For this reason, it is tagged "monthly"
since L2 data loads monthly.
*/
{{
    config(
        materialized="incremental",
        unique_key="LALVOTERID",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["monthly"],
    )
}}

with
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
            `Voters_CalculatedRegDate` as `CalculatedRegDate`,
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
            cast(
                to_date(`Voters_MovedFrom_Date`, 'MM/dd/yyyy') as date
            ) as `MovedFrom_Date`,
            `Voters_MovedFrom_Party_Description` as `MovedFrom_Party_Description`,
            `Voters_MovedFrom_State` as `MovedFrom_State`,
            `Voters_NameSuffix` as `NameSuffix`,
            `Voters_OfficialRegDate` as `OfficialRegDate`,
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
            (case when `AnyElection_2025` is true then 1 else 0 end)
            + (case when `General_2024` is true then 1 else 0 end)
            + (case when `Primary_2024` is true then 1 else 0 end)
            + (case when `PresidentialPrimary_2024` is true then 1 else 0 end)
            + (case when `OtherElection_2024` is true then 1 else 0 end)
            + (case when `AnyElection_2023` is true then 1 else 0 end)
            + (case when `General_2022` is true then 1 else 0 end)
            + (case when `Primary_2022` is true then 1 else 0 end)
            + (case when `OtherElection_2022` is true then 1 else 0 end)
            + (
                case when `AnyElection_2021` is true then 1 else 0 end
            ) as `Last_10_Elections_Voted`,
            (case when `AnyElection_2025` is true then 1 else 0 end)
            + (case when `General_2024` is true then 1 else 0 end)
            + (
                case when `Primary_2024` is true then 1 else 0 end
            ) as `Last_3_Elections_Voted`,
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
            `Voters_VotingPerformanceEvenYearGeneral`
            as `VotingPerformanceEvenYearGeneral`,
            `Voters_VotingPerformanceEvenYearGeneralAndPrimary`
            as `VotingPerformanceEvenYearGeneralAndPrimary`,
            `Voters_VotingPerformanceEvenYearPrimary`
            as `VotingPerformanceEvenYearPrimary`,
            `Voters_VotingPerformanceMinorElection` as `VotingPerformanceMinorElection`,

            -- Districts
            {{ get_l2_district_columns() }},
            -- District-adjacent columns outside get_l2_district_columns, kept
            -- for the people-api Voter contract
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
            `Proposed_Elementary_School_District`,
            `Proposed_Fire_District`,
            `Proposed_Unified_School_District`,
            `Republican_Area`,
            `Republican_Convention_Member`,
            `Vote_By_Mail_Area`,
            dbt_valid_from
        from {{ ref("snapshot__int__l2_nationwide_uniform") }}
        where
            1 = 1
            {% if is_incremental() %}
                and dbt_valid_from > (select max(updated_at) from {{ this }})  -- use dbt_valid_from to get true updated_at
            {% endif %}
        qualify
            row_number() over (partition by lalvoterid order by dbt_valid_from desc) = 1
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
                when tbl_updated.`Last_10_Elections_Voted` = 0
                then 'First Time'
                when tbl_updated.`Last_3_Elections_Voted` = 0
                then 'Unlikely'
                when tbl_updated.`Last_3_Elections_Voted` = 3
                then 'Super'
                when tbl_updated.`Last_3_Elections_Voted` = 2
                then 'Likely'
                else 'Unknown'
            end as `Voter_Status`,
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
            tbl_updated.`Proposed_Elementary_School_District`,
            tbl_updated.`Proposed_Fire_District`,
            tbl_updated.`Proposed_Unified_School_District`,
            tbl_updated.`Republican_Area`,
            tbl_updated.`Republican_Convention_Member`,
            tbl_updated.`Vote_By_Mail_Area`,
            {% if is_incremental() %}
                -- For incremental runs, preserve existing created_at for existing
                -- records,
                coalesce(
                    tbl_existing.created_at, tbl_updated.`dbt_valid_from`
                ) as created_at,
            {% else %}
                -- For full refresh, set created_at to dbt_valid_from for all records
                tbl_updated.`dbt_valid_from` as created_at,
            {% endif %}
            -- Always set updated_at to dbt_valid_from
            tbl_updated.`dbt_valid_from` as updated_at
        from updated_voters as tbl_updated
        {% if is_incremental() %}
            left join
                {{ this }} as tbl_existing
                on tbl_updated.`LALVOTERID` = tbl_existing.`LALVOTERID`
        {% endif %}
    )

select *
from final
