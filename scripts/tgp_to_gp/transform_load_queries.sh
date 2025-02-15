#!/bin/bash

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
