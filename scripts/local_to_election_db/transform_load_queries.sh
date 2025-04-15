#!/bin/bash

## place
place_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
    DROP TABLE IF EXISTS staging.\"Place\";
    CREATE TABLE staging.\"Place\" (
      id text NULL,
      created_at timestamp without time zone NULL,
      updated_at timestamp without time zone NULL,
      br_database_id integer NULL,
      name text NULL,
      slug text NULL,
      geoid text NULL,
      mtfcc text NULL,
      state text NULL,
      city_largest text NULL,
      county_name text NULL,
      population integer NULL,
      density real NULL,
      income_household_median integer NULL,
      unemployment_rate real NULL,
      home_value integer NULL,
      parent_id text NULL
    );"

place_upsert="
  INSERT INTO public.\"Place\" (
    id,
    created_at,
    updated_at,
    br_database_id,
    name,
    slug,
    geoid,
    mtfcc,
    state,
    city_largest,
    county_name,
    population,
    density,
    income_household_median,
    unemployment_rate,
    home_value,
    parent_id
  )
  SELECT
    id::uuid,
    created_at,
    updated_at,
    br_database_id,
    name,
    slug,
    geoid,
    mtfcc,
    state,
    city_largest,
    county_name,
    population,
    density,
    income_household_median,
    unemployment_rate,
    home_value,
    parent_id::uuid
  FROM staging.\"Place\"
    where geoid is not null
    and slug is not null
  ON CONFLICT (id) DO UPDATE SET
      created_at = EXCLUDED.created_at,
      updated_at = EXCLUDED.updated_at,
      br_database_id = EXCLUDED.br_database_id,
      name = EXCLUDED.name,
      slug = EXCLUDED.slug,
      geoid = EXCLUDED.geoid,
      mtfcc = EXCLUDED.mtfcc,
      state = EXCLUDED.state,
      city_largest = EXCLUDED.city_largest,
      county_name = EXCLUDED.county_name,
      population = EXCLUDED.population,
      density = EXCLUDED.density,
      income_household_median = EXCLUDED.income_household_median,
      unemployment_rate = EXCLUDED.unemployment_rate,
      home_value = EXCLUDED.home_value,
      parent_id = EXCLUDED.parent_id
;
"

## race
race_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
    DROP TABLE IF EXISTS staging.\"Race\";
    CREATE TABLE staging.\"Race\" (
      id uuid NOT NULL,
      created_at timestamp(3) without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at timestamp(3) without time zone NOT NULL,
      br_hash_id text NULL,
      br_database_id integer NULL,
      election_date timestamp(3) without time zone NOT NULL,
      state character(2) NOT NULL,
      position_level text NOT NULL,
      normalized_position_name text NULL,
      position_description text NULL,
      filing_office_address text NULL,
      filing_phone_number text NULL,
      paperwork_instructions text NULL,
      filing_requirements text NULL,
      is_runoff boolean NULL,
      is_primary boolean NULL,
      partisan_type text NULL,
      filing_date_start timestamp(3) without time zone NULL,
      filing_date_end timestamp(3) without time zone NULL,
      employment_type text NULL,
      eligibility_requirements text NULL,
      salary text NULL,
      sub_area_name text NULL,
      sub_area_value text NULL,
      frequency integer[] NULL,
      place_id uuid NULL,
      slug text NOT NULL,
      position_names text[] NULL
    );"

race_upsert="
  INSERT INTO public.\"Race\" (
    id,
    created_at,
    updated_at,
    br_hash_id,
    br_database_id,
    election_date,
    state,
    position_level,
    normalized_position_name,
    position_description,
    filing_office_address,
    filing_phone_number,
    paperwork_instructions,
    filing_requirements,
    is_runoff,
    is_primary,
    partisan_type,
    filing_date_start,
    filing_date_end,
    employment_type,
    eligibility_requirements,
    salary,
    sub_area_name,
    sub_area_value,
    frequency,
    place_id,
    slug,
    position_names
  )
  SELECT
    id,
    created_at,
    updated_at,
    br_hash_id,
    br_database_id,
    election_date,
    state,
    position_level::\"PositionLevel\",
    normalized_position_name,
    position_description,
    filing_office_address,
    filing_phone_number,
    paperwork_instructions,
    filing_requirements,
    is_runoff,
    is_primary,
    partisan_type,
    filing_date_start,
    filing_date_end,
    employment_type,
    eligibility_requirements,
    salary,
    sub_area_name,
    sub_area_value,
    frequency,
    place_id,
    slug,
    position_names
  FROM staging.\"Race\"
  ON CONFLICT (id) DO UPDATE SET
    br_hash_id = EXCLUDED.br_hash_id,
    br_database_id = EXCLUDED.br_database_id,
    election_date = EXCLUDED.election_date,
    state = EXCLUDED.state,
    position_level = EXCLUDED.position_level,
    normalized_position_name = EXCLUDED.normalized_position_name,
    position_description = EXCLUDED.position_description,
    filing_office_address = EXCLUDED.filing_office_address,
    filing_phone_number = EXCLUDED.filing_phone_number,
    paperwork_instructions = EXCLUDED.paperwork_instructions,
    filing_requirements = EXCLUDED.filing_requirements,
    is_runoff = EXCLUDED.is_runoff,
    is_primary = EXCLUDED.is_primary,
    partisan_type = EXCLUDED.partisan_type,
    filing_date_start = EXCLUDED.filing_date_start,
    filing_date_end = EXCLUDED.filing_date_end,
    employment_type = EXCLUDED.employment_type,
    eligibility_requirements = EXCLUDED.eligibility_requirements,
    salary = EXCLUDED.salary,
    sub_area_name = EXCLUDED.sub_area_name,
    sub_area_value = EXCLUDED.sub_area_value,
    frequency = EXCLUDED.frequency,
    place_id = EXCLUDED.place_id,
    slug = EXCLUDED.slug,
    position_names = EXCLUDED.position_names,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
"
