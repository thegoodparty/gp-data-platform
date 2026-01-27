{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="district_id",
        on_schema_change="fail",
        tags=["mart", "people_api", "district_stats"],
    )
}}

-- databricks_compute='serverless_medium', -- 310 s
-- default xxs serverless compute: 1950 s
-- default xs serverless compute: ~1070 s (18 min)
/*
This model creates district statistics by aggregating voter demographic data per district.
It computes bucket distributions for age, homeowner status, education, presence of children,
and estimated income range for each district.

Output schema matches:
    - district_id: String (primary key)
    - updated_at: DateTime
    - total_constituents: Int
    - total_constituents_with_cell_phone: Int
    - buckets: Struct containing:
        - age: Array of Bucket structs
        - homeowner: Array of Bucket structs
        - education: Array of Bucket structs
        - presenceOfChildren: Array of Bucket structs
        - estimatedIncomeRange: Array of Bucket structs

    Where Bucket is a Struct with:
        - label: String
        - count: Long
        - percent: Double

See: https://github.com/thegoodparty/people-api/tree/develop/prisma/schema/DistrictStats.prisma
*/
with
    -- Incremental logic: filter to only districts with updated voters
    {% if is_incremental() %}
        max_updated_at as (select max(updated_at) as max_updated_at from {{ this }}),
        updated_voter_ids as (
            select distinct voter.id as voter_id
            from {{ ref("m_people_api__voter") }} as voter
            cross join max_updated_at
            where
                max_updated_at.max_updated_at is null
                or voter.updated_at > max_updated_at.max_updated_at
        ),
        updated_district_ids as (
            select distinct districtvoter.district_id
            from {{ ref("m_people_api__districtvoter") }} as districtvoter
            inner join
                updated_voter_ids on districtvoter.voter_id = updated_voter_ids.voter_id
        ),
        filtered_districtvoter as (
            select districtvoter.*
            from {{ ref("m_people_api__districtvoter") }} as districtvoter
            inner join
                updated_district_ids
                on districtvoter.district_id = updated_district_ids.district_id
        ),
    {% else %}
        filtered_districtvoter as (
            select * from {{ ref("m_people_api__districtvoter") }}
        ),
    {% endif %}

    -- Get state-level district IDs for statewide positions (Governor, US Senate, etc.)
    state_districts as (
        select id as district_id, state
        from {{ ref("m_people_api__district") }}
        where type = 'State'
    ),

    -- Join districtvoter with voter data to get demographics per district
    voters_with_districts as (
        -- Existing district-level voter associations
        select
            districtvoter.district_id,
            districtvoter.voter_id,
            voter.age_int,
            voter.homeowner_probability_model,
            voter.education_of_person,
            voter.presence_of_children,
            voter.estimated_income_amount_int,
            voter.votertelephones_cellphoneformatted,
            voter.updated_at
        from filtered_districtvoter as districtvoter
        inner join
            {{ ref("m_people_api__voter") }} as voter
            on districtvoter.voter_id = voter.id

        union all

        -- State-level voter associations for statewide positions
        select
            state_districts.district_id,
            voter.id as voter_id,
            voter.age_int,
            voter.homeowner_probability_model,
            voter.education_of_person,
            voter.presence_of_children,
            voter.estimated_income_amount_int,
            voter.votertelephones_cellphoneformatted,
            voter.updated_at
        from {{ ref("m_people_api__voter") }} as voter
        inner join state_districts on voter.state = state_districts.state
        {% if is_incremental() %}
            inner join updated_voter_ids on voter.id = updated_voter_ids.voter_id
        {% endif %}
    ),

    -- Add age and income bucket label columns
    voters_with_buckets as (
        select
            *,
            case
                when age_int >= 18 and age_int <= 25
                then '18-25'
                when age_int >= 26 and age_int <= 35
                then '26-35'
                when age_int >= 36 and age_int <= 50
                then '36-50'
                when age_int >= 51
                then '51+'
                else 'Unknown'
            end as age_bucket,
            case
                when
                    estimated_income_amount_int >= 1000
                    and estimated_income_amount_int < 15000
                then '1k–15k'
                when
                    estimated_income_amount_int >= 15000
                    and estimated_income_amount_int < 25000
                then '15k–25k'
                when
                    estimated_income_amount_int >= 25000
                    and estimated_income_amount_int < 35000
                then '25k–35k'
                when
                    estimated_income_amount_int >= 35000
                    and estimated_income_amount_int < 50000
                then '35k–50k'
                when
                    estimated_income_amount_int >= 50000
                    and estimated_income_amount_int < 75000
                then '50k–75k'
                when
                    estimated_income_amount_int >= 75000
                    and estimated_income_amount_int < 100000
                then '75k–100k'
                when
                    estimated_income_amount_int >= 100000
                    and estimated_income_amount_int < 125000
                then '100k–125k'
                when
                    estimated_income_amount_int >= 125000
                    and estimated_income_amount_int < 150000
                then '125k–150k'
                when
                    estimated_income_amount_int >= 150000
                    and estimated_income_amount_int < 175000
                then '150k–175k'
                when
                    estimated_income_amount_int >= 175000
                    and estimated_income_amount_int < 200000
                then '175k–200k'
                when
                    estimated_income_amount_int >= 200000
                    and estimated_income_amount_int < 250000
                then '200k–250k'
                when estimated_income_amount_int >= 250000
                then '250k+'
                else 'Unknown'
            end as income_bucket
        from voters_with_districts
    ),

    -- Compute total constituents per district and max updated_at from voters
    district_totals as (
        select
            district_id,
            count(*) as total_constituents,
            sum(
                case
                    when
                        votertelephones_cellphoneformatted is not null
                        and trim(votertelephones_cellphoneformatted) != ''
                    then 1
                    else 0
                end
            ) as total_constituents_with_cell_phone,
            max(updated_at) as updated_at
        from voters_with_buckets
        group by district_id
    ),

    -- Age bucket aggregation
    age_counts as (
        select district_id, age_bucket as label, count(*) as count
        from voters_with_buckets
        group by district_id, age_bucket
    ),
    age_buckets_with_percent as (
        select
            age_counts.district_id,
            coalesce(age_counts.label, 'Unknown') as label,
            age_counts.count,
            round(
                (age_counts.count / district_totals.total_constituents) * 100, 2
            ) as percent
        from age_counts
        inner join
            district_totals on age_counts.district_id = district_totals.district_id
    ),
    age_buckets as (
        select
            district_id,
            sort_array(collect_list(struct(label, count, percent)), false) as age
        from age_buckets_with_percent
        group by district_id
    ),

    -- Homeowner bucket aggregation with label mapping
    homeowner_counts_raw as (
        select district_id, homeowner_probability_model as label, count(*) as count
        from voters_with_buckets
        group by district_id, homeowner_probability_model
    ),
    homeowner_counts_mapped as (
        select
            district_id,
            case
                when label in ('Home Owner', 'Probable Home Owner')
                then 'Yes'
                when label = 'Renter'
                then 'No'
                else 'Unknown'
            end as label,
            count
        from homeowner_counts_raw
    ),
    homeowner_counts as (
        select district_id, label, sum(count) as count
        from homeowner_counts_mapped
        group by district_id, label
    ),
    homeowner_buckets_with_percent as (
        select
            homeowner_counts.district_id,
            coalesce(homeowner_counts.label, 'Unknown') as label,
            homeowner_counts.count,
            round(
                (homeowner_counts.count / district_totals.total_constituents) * 100, 2
            ) as percent
        from homeowner_counts
        inner join
            district_totals
            on homeowner_counts.district_id = district_totals.district_id
    ),
    homeowner_buckets as (
        select
            district_id,
            sort_array(collect_list(struct(label, count, percent)), false) as homeowner
        from homeowner_buckets_with_percent
        group by district_id
    ),

    -- Education bucket aggregation with label mapping
    education_counts_raw as (
        select district_id, education_of_person as label, count(*) as count
        from voters_with_buckets
        group by district_id, education_of_person
    ),
    education_counts_mapped as (
        select
            district_id,
            case
                when label = 'Did Not Complete High School Likely'
                then 'None'
                when label = 'Unknown'
                then 'Unknown'
                when label = 'Attended Vocational/Technical School Likely'
                then 'Technical School'
                when label = 'Completed Graduate School Likely'
                then 'Graduate Degree'
                when label = 'Completed College Likely'
                then 'College Degree'
                when label = 'Completed High School Likely'
                then 'High School Diploma'
                when label = 'Attended But Did Not Complete College Likely'
                then 'Some College'
                else 'Unknown'
            end as label,
            count
        from education_counts_raw
    ),
    education_counts as (
        select district_id, label, sum(count) as count
        from education_counts_mapped
        group by district_id, label
    ),
    education_buckets_with_percent as (
        select
            education_counts.district_id,
            coalesce(education_counts.label, 'Unknown') as label,
            education_counts.count,
            round(
                (education_counts.count / district_totals.total_constituents) * 100, 2
            ) as percent
        from education_counts
        inner join
            district_totals
            on education_counts.district_id = district_totals.district_id
    ),
    education_buckets as (
        select
            district_id,
            sort_array(collect_list(struct(label, count, percent)), false) as education
        from education_buckets_with_percent
        group by district_id
    ),

    -- Presence of children bucket aggregation with label mapping
    children_counts_raw as (
        select district_id, presence_of_children as label, count(*) as count
        from voters_with_buckets
        group by district_id, presence_of_children
    ),
    children_counts_mapped as (
        select
            district_id,
            case
                when label = 'Y' then 'Yes' when label = 'N' then 'No' else 'Unknown'
            end as label,
            count
        from children_counts_raw
    ),
    children_counts as (
        select district_id, label, sum(count) as count
        from children_counts_mapped
        group by district_id, label
    ),
    children_buckets_with_percent as (
        select
            children_counts.district_id,
            coalesce(children_counts.label, 'Unknown') as label,
            children_counts.count,
            round(
                (children_counts.count / district_totals.total_constituents) * 100, 2
            ) as percent
        from children_counts
        inner join
            district_totals on children_counts.district_id = district_totals.district_id
    ),
    presence_of_children_buckets as (
        select
            district_id,
            sort_array(
                collect_list(struct(label, count, percent)), false
            ) as presenceofchildren
        from children_buckets_with_percent
        group by district_id
    ),

    -- Income bucket aggregation
    income_counts as (
        select district_id, income_bucket as label, count(*) as count
        from voters_with_buckets
        group by district_id, income_bucket
    ),
    income_buckets_with_percent as (
        select
            income_counts.district_id,
            coalesce(income_counts.label, 'Unknown') as label,
            income_counts.count,
            round(
                (income_counts.count / district_totals.total_constituents) * 100, 2
            ) as percent
        from income_counts
        inner join
            district_totals on income_counts.district_id = district_totals.district_id
    ),
    income_buckets as (
        select
            district_id,
            sort_array(
                collect_list(struct(label, count, percent)), false
            ) as estimatedincomerange
        from income_buckets_with_percent
        group by district_id
    ),

    -- Join all bucket dataframes together
    result as (
        select
            district_totals.district_id,
            district_totals.updated_at,
            district_totals.total_constituents,
            district_totals.total_constituents_with_cell_phone,
            age_buckets.age,
            homeowner_buckets.homeowner,
            education_buckets.education,
            presence_of_children_buckets.presenceofchildren,
            income_buckets.estimatedincomerange
        from district_totals
        left join age_buckets on district_totals.district_id = age_buckets.district_id
        left join
            homeowner_buckets
            on district_totals.district_id = homeowner_buckets.district_id
        left join
            education_buckets
            on district_totals.district_id = education_buckets.district_id
        left join
            presence_of_children_buckets
            on district_totals.district_id = presence_of_children_buckets.district_id
        left join
            income_buckets on district_totals.district_id = income_buckets.district_id
    )

-- Create the final buckets struct
select
    district_id,
    updated_at,
    total_constituents,
    total_constituents_with_cell_phone,
    struct(
        age as age,
        homeowner as homeowner,
        education as education,
        presenceofchildren as presenceofchildren,
        estimatedincomerange as estimatedincomerange
    ) as buckets
from result
