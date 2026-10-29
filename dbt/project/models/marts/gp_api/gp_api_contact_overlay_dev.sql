-- Live over gp-api Postgres through federation. Joining on organization_slug as
-- well as the upload id lets a reader's org filter reach both federated scans.
with
    upload_rows as (
        select
            matches.organization_slug,
            matches.person_id,
            matches.is_new_person,
            matches.first_name,
            matches.last_name,
            matches.email,
            matches.cell_phone,
            matches.landline,
            matches.address_line,
            matches.city,
            matches.zip,
            uploads.created_at as uploaded_at,
            -- Two rows of one file can point at the same person; the later row
            -- wins, so a value never flips between reads.
            struct(uploads.created_at, matches.row_index) as upload_order
        from {{ source("gp_api_pg_dev", "contact_upload_match") }} as matches
        inner join
            {{ source("gp_api_pg_dev", "contact_upload") }} as uploads
            on matches.upload_id = uploads.id
            and matches.organization_slug = uploads.organization_slug
        where matches.person_id is not null
    )

select
    organization_slug,
    person_id as id,
    -- Once any row points an L2 id at a person, they are not new.
    bool_and(is_new_person) as is_new,
    max_by(first_name, upload_order) filter (
        where first_name is not null
    ) as first_name,
    max_by(last_name, upload_order) filter (where last_name is not null) as last_name,
    max_by(email, upload_order) filter (where email is not null) as email,
    max_by(cell_phone, upload_order) filter (
        where cell_phone is not null
    ) as cell_phone,
    max_by(landline, upload_order) filter (where landline is not null) as landline,
    max_by(address_line, upload_order) filter (
        where address_line is not null
    ) as address_line,
    max_by(city, upload_order) filter (where city is not null) as city,
    max_by(zip, upload_order) filter (where zip is not null) as zip,
    max(uploaded_at) as last_uploaded_at
from upload_rows
group by organization_slug, person_id
