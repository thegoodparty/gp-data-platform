-- callback_number (a phone number) and the Stripe ids are left out on purpose:
-- analysis does not need them.
with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_outreach_robocall") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("outreach_id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("settle_state") }},
            {{ adapter.quote("audio_key") }},
            {{ adapter.quote("billable_count") }},
            {{ adapter.quote("amount_in_cents") }},
            {{ adapter.quote("authorized_amount_in_cents") }},
            {{ adapter.quote("captured_amount_in_cents") }},
            {{ adapter.quote("pay_attempt") }},
            {{ adapter.quote("capture_before") }},
            {{ adapter.quote("promo_code") }},
            {{ adapter.quote("promo_discount_in_cents") }},
            {{ adapter.quote("promo_redeemed_at") }},
            {{ adapter.quote("promo_covers_total") }},
            {{ adapter.quote("callhub_campaign_pk_str") }},
            {{ adapter.quote("staged_calls_per_minute") }},
            {{ adapter.quote("callhub_starting_date") }},
            {{ adapter.quote("callhub_expiration_date") }},
            {{ adapter.quote("compliance_passed_at") }},
            {{ adapter.quote("compliance_audio_etag") }},
            {{ adapter.quote("dialing_started_at") }},
            {{ adapter.quote("dialed_at") }},
            {{ adapter.quote("permanent_send_failure") }},
            {{ adapter.quote("completed_call_count") }},
            {{ adapter.quote("completion_polled_at") }},
            {{ adapter.quote("pool_freed_at") }},
            {{ adapter.quote("overdue_alerted_at") }},
            {{ adapter.quote("backlog_alerted_at") }},
            {{ adapter.quote("last_reminder_sent_at") }}
        from source
    )
select *
from renamed
