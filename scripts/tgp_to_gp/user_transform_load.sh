#!/bin/bash
set +e  # Don't exit on error
trap 'read -p "Command failed. Press enter to continue..."' ERR


# Parse command line arguments
db_host=""
db_port=""
db_user=""
db_name=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db_host)
            db_host="$2"
            shift 2
            ;;
        --db_port)
            db_port="$2"
            shift 2
            ;;
        --db_user)
            db_user="$2"
            shift 2
            ;;
        --db_name)
            db_name="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --db_host hostname --db_port port --db_user username --db_name dbname"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$db_host" ] || [ -z "$db_port" ] || [ -z "$db_user" ] || [ -z "$db_name" ]; then
    echo "Error: Missing required database connection parameters"
    echo "Use --help for usage information"
    exit 1
fi

# Check if GP_PGPASSWORD environment variable is set
if [ -z "${GP_PGPASSWORD}" ]; then
    echo "Error: GP_PGPASSWORD environment variable is not set"
    echo "Please set it using: export GP_PGPASSWORD='your_password'"
    exit 1
fi

# Set PGPASSWORD for this script's duration
export PGPASSWORD="${GP_PGPASSWORD}"

# Start the timer
start_time=$(date +%s)
echo "Timer started."

# Start the timer for schema creation
echo "Creating staging table schema..."
schema_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.user;
  CREATE TABLE staging.user (
    created_at bigint NULL,
    updated_at bigint NULL,
    id serial NOT NULL,
    phone text NULL,
    email text NULL,
    uuid text NULL,
    name text NULL,
    feedback text NULL,
    social_id text NULL,
    social_provider text NULL,
    display_address text NULL,
    address_components text NULL,
    normalized_address text NULL,
    is_phone_verified boolean NULL,
    is_email_verified boolean NULL,
    avatar text NULL,
    email_conf_token text NULL,
    email_conf_token_date_created text NULL,
    presidential_rank text NULL,
    senate_rank text NULL,
    house_rank text NULL,
    short_state text NULL,
    district_number real NULL,
    guest_referrer text NULL,
    role text NULL,
    cong_district integer NULL,
    house_district integer NULL,
    senate_district integer NULL,
    zip_code integer NULL,
    referrer integer NULL,
    is_admin boolean NULL,
    crew_count real NOT NULL DEFAULT 0,
    password text NOT NULL DEFAULT ''::text,
    password_reset_token text NOT NULL DEFAULT ''::text,
    password_reset_token_expires_at real NOT NULL DEFAULT 0,
    has_password boolean NOT NULL DEFAULT false,
    vote_status text NOT NULL DEFAULT ''::text,
    first_name text NOT NULL DEFAULT ''::text,
    middle_name text NOT NULL DEFAULT ''::text,
    last_name text NOT NULL DEFAULT ''::text,
    suffix text NOT NULL DEFAULT ''::text,
    dob text NOT NULL DEFAULT ''::text,
    address text NOT NULL DEFAULT ''::text,
    city text NOT NULL DEFAULT ''::text,
    meta_data text NULL,
    candidate integer NULL,
    zip text NOT NULL DEFAULT ''::text,
    display_name text NOT NULL DEFAULT ''::text,
    pronouns text NOT NULL DEFAULT ''::text
  );"
schema_end=$(date +%s)
schema_time=$((schema_end - schema_start))
printf "Schema creation completed in %02d:%02d:%02d\n" $((schema_time/3600)) $((schema_time/60%60)) $((schema_time%60))

# Start the timer for data upload
echo "Uploading data to staging table..."
upload_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "\COPY staging.user FROM './tmp_data/user.csv' WITH CSV HEADER"
upload_end=$(date +%s)
upload_time=$((upload_end - upload_start))
printf "Data upload completed in %02d:%02d:%02d\n" $((upload_time/3600)) $((upload_time/60%60)) $((upload_time%60))

## upsert with transforms into destination table
echo "Upserting data into destination table..."
upsert_start=$(date +%s)
sql_command="
    INSERT INTO public.user (
        id,
        created_at,
        updated_at,
        first_name,
        last_name,
        name,
        password,
        email,
        phone,
        zip,
        meta_data,
        roles,
        password_reset_token,
        avatar,
        has_password
    )
    SELECT
        id,
        to_timestamp(created_at::double precision/1000),
        to_timestamp(updated_at::double precision/1000),
        first_name,
        last_name,
        NULL,
        password,
        email,
        phone,
        zip,
        CASE
            WHEN meta_data = '' THEN '{}'
            ELSE meta_data
        END::jsonb as meta_data,
        CASE
            WHEN role = 'campaign' THEN ARRAY['candidate'::\"UserRole\"]
            ELSE ARRAY[role::\"UserRole\"]
        END as roles,
        password_reset_token,
        avatar,
        has_password
    FROM staging.user
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        name = EXCLUDED.name,
        password = EXCLUDED.password,
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        zip = EXCLUDED.zip,
        meta_data = EXCLUDED.meta_data,
        roles = EXCLUDED.roles,
        password_reset_token = EXCLUDED.password_reset_token,
        avatar = EXCLUDED.avatar,
        has_password = EXCLUDED.has_password;
"
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$sql_command"
upsert_end=$(date +%s)
upsert_time=$((upsert_end - upsert_start))
printf "Upsert completed in %02d:%02d:%02d\n" $((upsert_time/3600)) $((upsert_time/60%60)) $((upsert_time%60))

## drop the staging table
echo "Dropping staging table..."
drop_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "DROP TABLE staging.user;"
drop_end=$(date +%s)
drop_time=$((drop_end - drop_start))
printf "Staging table drop completed in %02d:%02d:%02d\n" $((drop_time/3600)) $((drop_time/60%60)) $((drop_time%60))

# Unset PGPASSWORD for security
unset PGPASSWORD

## end timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
echo "Timer stopped."

# Calculate and print the elapsed time
seconds=$((elapsed_time % 60))
minutes=$((elapsed_time / 60 % 60))
hours=$((elapsed_time / 3600))

printf "Elapsed time: %02d:%02d:%02d\n" $hours $minutes $seconds
