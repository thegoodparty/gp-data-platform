#!/bin/bash

## user
user_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
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

user_upsert="
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
      case
        when meta_data = '' then '{}'
        else meta_data
      end::jsonb as meta_data,
      case
        when role = 'campaign' and is_admin is true then array['candidate'::\"UserRole\", 'admin'::\"UserRole\"]
        when role = 'campaign' and is_admin is false then array['candidate'::\"UserRole\"]
        when is_admin is true then array[role::\"UserRole\", 'admin'::\"UserRole\"]
        else array[role::\"UserRole\"]
      end as roles,
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

## campaign
campaign_create_staging="
CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.campaign;
  CREATE TABLE staging.campaign (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    slug text NULL,
    \"isActive\" boolean NULL,
    data json NULL,
    \"user\" integer NULL,
    \"isVerified\" boolean NULL,
    \"isPro\" boolean NULL,
    \"didWin\" boolean NULL,
    tier text NULL,
    \"dateVerified\" date NULL,
    \"dataCopy\" json NULL,
    details jsonb NULL,
    \"pathToVictory\" integer NULL,
    \"aiContent\" jsonb NULL,
    \"ballotCandidate\" integer NULL,
    \"isDemo\" boolean NULL DEFAULT false,
    \"vendorTsData\" jsonb NULL DEFAULT '{}'::jsonb
  );"

campaign_upsert="
    INSERT INTO public.campaign (
        id,
        created_at,
        updated_at,
        slug,
        is_active,
        is_verified,
        is_pro,
        is_demo,
        did_win,
        date_verified,
        tier,
        data,
        details,
        ai_content,
        vendor_ts_data,
        user_id
    )
    SELECT
        id,
        to_timestamp(\"createdAt\"::double precision/1000),
        to_timestamp(\"updatedAt\"::double precision/1000),
        slug,
        \"isActive\",
        \"isVerified\",
        \"isPro\",
        \"isDemo\",
        \"didWin\",
        \"dateVerified\",
        tier::\"CampaignTier\",
        COALESCE(data::jsonb, '{}'::jsonb),
        COALESCE(details, '{}'::jsonb),
        COALESCE(\"aiContent\", '{}'::jsonb),
        COALESCE(\"vendorTsData\", '{}'::jsonb),
        \"user\"
    FROM staging.campaign
    WHERE \"user\" IN (SELECT id FROM public.user)
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        slug = EXCLUDED.slug,
        is_active = EXCLUDED.is_active,
        is_verified = EXCLUDED.is_verified,
        is_pro = EXCLUDED.is_pro,
        is_demo = EXCLUDED.is_demo,
        did_win = EXCLUDED.did_win,
        date_verified = EXCLUDED.date_verified,
        tier = EXCLUDED.tier,
        data = EXCLUDED.data,
        details = EXCLUDED.details,
        ai_content = EXCLUDED.ai_content,
        vendor_ts_data = EXCLUDED.vendor_ts_data,
        user_id = EXCLUDED.user_id;
"


## aichat
aichat_create_staging="
  DROP TABLE IF EXISTS staging.aichat;
  CREATE TABLE staging.aichat (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    assistant text NULL,
    thread text NULL,
    data json NULL,
    \"user\" integer NULL,
    campaign integer NULL
  );"

aichat_upsert="
  INSERT INTO public.ai_chat (
      created_at,
      updated_at,
      id,
      assistant,
      thread_id,
      data,
      user_id,
      campaign_id
  )
  SELECT
      to_timestamp(\"createdAt\"::double precision/1000),
      to_timestamp(\"updatedAt\"::double precision/1000),
      id,
      assistant,
      thread,
      data,
      \"user\",
      campaign
  FROM staging.aichat
  WHERE campaign in (select id from public.campaign)
  ON CONFLICT (id) DO UPDATE SET
      created_at = EXCLUDED.created_at,
      updated_at = EXCLUDED.updated_at,
      assistant = EXCLUDED.assistant,
      thread_id = EXCLUDED.thread_id,
      data = EXCLUDED.data,
      user_id = EXCLUDED.user_id,
      campaign_id = EXCLUDED.campaign_id
;"


## campaignplanversion
campaignplanversion_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.campaignplanversion;
  CREATE TABLE staging.campaignplanversion (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    data json NULL,
    campaign integer NULL
  );"

campaignplanversion_upsert="
  INSERT INTO public.campaign_plan_version (
    id,
    campaign_id,
    data,
    created_at,
    updated_at
  )
  SELECT
    id,
    campaign,
    data,
    to_timestamp(\"createdAt\"::double precision/1000),
    to_timestamp(\"updatedAt\"::double precision/1000)
  FROM staging.campaignplanversion
  WHERE campaign IN (SELECT id FROM public.campaign)
  ON CONFLICT (id) DO UPDATE SET
    campaign_id = EXCLUDED.campaign_id,
    data = EXCLUDED.data,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
"

## topissue
topissue_create_staging="
CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.topissue;
  CREATE TABLE staging.topissue (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    name text NULL,
    icon text NULL
  );"

topissue_upsert="
  INSERT INTO public.top_issue (
    name,
    id,
    created_at,
    updated_at
  )
  SELECT
    name,
    id,
    to_timestamp(\"createdAt\"::double precision/1000) AS created_at,
    to_timestamp(\"updatedAt\"::double precision/1000) AS updated_at
  FROM staging.topissue
  ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
"


## campaign_topIssues__topissue_campaigns
campaign_topIssues__topissue_campaigns_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.\"campaign_topIssues__topissue_campaigns\";
  CREATE TABLE staging.\"campaign_topIssues__topissue_campaigns\" (
    id serial NOT NULL,
    \"campaign_topIssues\" integer NULL,
    \"topissue_campaigns\" integer NULL
  );"

campaign_topIssues__topissue_campaigns_upsert="
  INSERT INTO public.\"_CampaignToTopIssue\" (
    \"A\",
    \"B\"
  )
  SELECT
    \"campaign_topIssues\",
    \"topissue_campaigns\"
  FROM staging.\"campaign_topIssues__topissue_campaigns\"
  WHERE 1=1
    AND \"campaign_topIssues\" IN (SELECT id FROM public.campaign)
    AND \"topissue_campaigns\" IN (SELECT id FROM public.top_issue)
  ON CONFLICT (\"A\", \"B\") DO NOTHING
;"


## position
position_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.position;
  CREATE TABLE staging.position (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    name text NULL,
    \"topIssue\" integer NULL
  );"

position_upsert="
  INSERT INTO public.position (
    name,
    id,
    top_issue_id,
    created_at,
    updated_at
  )
  SELECT
    name,
    id,
    \"topIssue\",
    to_timestamp(\"createdAt\"::double precision/1000),
    to_timestamp(\"updatedAt\"::double precision/1000)
  FROM staging.position
  ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    top_issue_id = EXCLUDED.top_issue_id,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
"


## candidateposition
candidateposition_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.candidateposition;
  CREATE TABLE staging.candidateposition (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    description text NULL,
    \"order\" real NULL,
    candidate integer NULL,
    \"topIssue\" integer NULL,
    \"position\" integer NULL,
    campaign integer NULL
  );"

candidateposition_upsert="
  INSERT INTO public.campaign_position (
    description,
    \"order\",
    id,
    campaign_id,
    position_id,
    top_issue_id,
    created_at,
    updated_at
  )
  SELECT
    description,
    \"order\",
    id,
    campaign,
    \"position\",
    \"topIssue\",
    to_timestamp(\"createdAt\"::double precision/1000),
    to_timestamp(\"updatedAt\"::double precision/1000)
  FROM staging.candidateposition
  WHERE campaign IN (SELECT id FROM public.campaign)
    and \"topIssue\" in (select id from public.top_issue)
    and \"position\" in (select id from public.position)
  ON CONFLICT (id) DO UPDATE SET
    description = EXCLUDED.description,
    \"order\" = EXCLUDED.\"order\",
    campaign_id = EXCLUDED.campaign_id,
    position_id = EXCLUDED.position_id,
    top_issue_id = EXCLUDED.top_issue_id,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
"

## campaignupdatehistory
campaignupdatehistory_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.campaignupdatehistory;
  CREATE TABLE staging.campaignupdatehistory (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    \"type\" text NULL,
    quantity real NULL,
    \"user\" integer NULL,
    campaign integer NULL
  );"

campaignupdatehistory_upsert="
  INSERT INTO public.campaign_update_history (
    id,
    created_at,
    updated_at,
    campaign_id,
    user_id,
    \"type\",
    quantity
  )
  SELECT
    id,
    to_timestamp(\"createdAt\"::double precision/1000),
    to_timestamp(\"updatedAt\"::double precision/1000),
    campaign,
    \"user\",
    \"type\"::\"CampaignUpdateHistoryType\",
    quantity::integer
  FROM staging.campaignupdatehistory
  WHERE 1=1
    and quantity <= 2147483647
    and campaign in (select id from public.campaign)
  ON CONFLICT (id) DO UPDATE SET
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    campaign_id = EXCLUDED.campaign_id,
    user_id = EXCLUDED.user_id,
    \"type\" = EXCLUDED.\"type\",
    quantity = EXCLUDED.quantity;
"


## censusentity
censusentity_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.censusentity;
  CREATE TABLE staging.censusentity (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    mtfcc text NULL,
    \"mtfccType\" text NULL,
    \"geoId\" text NULL,
    name text NULL,
    state text NULL
  );"

censusentity_upsert="
  INSERT INTO public.census_entity (
    id,
    mtfcc,
    mtfcc_type,
    geo_id,
    name,
    state,
    created_at,
    updated_at
  )
  SELECT
    id,
    mtfcc,
    \"mtfccType\",
    \"geoId\",
    name,
    state,
    to_timestamp(\"createdAt\"::double precision/1000),
    to_timestamp(\"updatedAt\"::double precision/1000)
  FROM staging.censusentity
  ON CONFLICT (id) DO UPDATE SET
    mtfcc = EXCLUDED.mtfcc,
    mtfcc_type = EXCLUDED.mtfcc_type,
    geo_id = EXCLUDED.geo_id,
    name = EXCLUDED.name,
    state = EXCLUDED.state,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
"

## county
county_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.county;
  CREATE TABLE staging.county (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    slug text NULL,
    name text NULL,
    state text NULL,
    data json NULL
  );"

county_upsert="
  INSERT INTO public.county (
    id,
    slug,
    name,
    state,
    data,
    created_at,
    updated_at
  )
  SELECT
    id,
    slug,
    name,
    state,
    data,
    to_timestamp(\"createdAt\"::double precision/1000),
    to_timestamp(\"updatedAt\"::double precision/1000)
  FROM staging.county
  ON CONFLICT (id) DO UPDATE SET
    slug = EXCLUDED.slug,
    name = EXCLUDED.name,
    state = EXCLUDED.state,
    data = EXCLUDED.data,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
"


## pathtovictory
pathtovictory_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.pathtovictory;
  CREATE TABLE staging.pathtovictory (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    data json NULL,
    campaign integer NULL
  );"

pathtovictory_upsert="
  INSERT INTO public.path_to_victory (
    id,
    created_at,
    updated_at,
    campaign_id,
    data
  )
  SELECT
    id,
    to_timestamp(\"createdAt\"::double precision/1000),
    to_timestamp(\"updatedAt\"::double precision/1000),
    campaign,
    data
  FROM staging.pathtovictory
  WHERE campaign IN (SELECT id FROM public.campaign)
  ON CONFLICT (id) DO UPDATE SET
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    campaign_id = EXCLUDED.campaign_id,
    data = EXCLUDED.data;
"


## municipality
municipality_create_staging="
  CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.municipality;
  CREATE TABLE staging.municipality (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    slug text NULL,
    name text NULL,
    type text NULL,
    state text NULL,
    data json NULL,
    county integer NULL
  );"

municipality_upsert="
  INSERT INTO public.municipality (
    id,
    slug,
    name,
    type,
    state,
    data,
    county_id,
    created_at,
    updated_at
  )
  SELECT
    id,
    slug,
    name,
    type::\"MunicipalityType\",
    state,
    data,
    county,
    to_timestamp(\"createdAt\"::double precision/1000),
    to_timestamp(\"updatedAt\"::double precision/1000)
  FROM staging.municipality
  ON CONFLICT (id) DO UPDATE SET
    slug = EXCLUDED.slug,
    name = EXCLUDED.name,
    type = EXCLUDED.type,
    state = EXCLUDED.state,
    data = EXCLUDED.data,
    county_id = EXCLUDED.county_id,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
"
