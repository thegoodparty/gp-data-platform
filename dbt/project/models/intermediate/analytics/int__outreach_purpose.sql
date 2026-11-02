-- What each outreach send was for: one row per gp-api outreach row, with the
-- best available purpose and where it came from.
--
-- Every outreach flow asks "What do you want to do?", but the answer was kept
-- only in some places: on door turfs, call lists and social posts, and on the
-- Outreach - Campaign Created/Completed events. Everything else is read from
-- the message text by a model, offline, and frozen in the
-- outreach_purpose_labels seed with a few hand readings, because a model call
-- per build would relabel every send on each full refresh.
--
-- Precedence, highest first: a hand reading, the sender's pick (custom says
-- nothing about the content, so it falls through), the legacy template key,
-- the recommended list's intent, then the model's reading. A model reading
-- counts when it was confident, or somewhat confident on a purpose where hand
-- readings agreed with it.
{{ config(materialized="table") }}

with
    sends as (
        select
            o.id as outreach_id,
            o.outreach_type,
            if(o.organization_slug like 'eo-%', 'serve', 'win') as product,
            o.status,
            o.createdat as created_at,
            o.campaignid as campaign_id,
            o.organization_slug,
            o.voter_file_filter_id,
            o.phone_banking_list_id,
            o.door_knocking_turf_id,
            -- Before scripts held text, a request stored the Content Builder
            -- template it came from as a one-word key, e.g. smsPersuasive2.
            case
                when trim(o.script) rlike '^[A-Za-z0-9_]*$'
                then nullif(regexp_replace(trim(o.script), '[0-9]+$', ''), '')
            end as legacy_key,
            length(
                coalesce(
                    nullif(trim(o.message), ''),
                    nullif(trim(o.script), ''),
                    nullif(trim(pb.script), '')
                )
            )
            > 20 as has_text
        from {{ ref("stg_airbyte_source__gp_api_db_outreach") }} as o
        left join
            {{ ref("stg_airbyte_source__gp_api_db_phone_banking_list") }} as pb
            on pb.id = o.phone_banking_list_id
    ),

    event_picks as (
        select
            cast(
                get_json_object(
                    cast(event_properties as string), '$.outreachCampaignId'
                ) as bigint
            ) as outreach_id,
            max_by(
                get_json_object(cast(event_properties as string), '$.purpose'),
                event_time
            ) as purpose
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where
            event_type
            in ('Outreach - Campaign Created', 'Outreach - Campaign Completed')
            and get_json_object(cast(event_properties as string), '$.purpose')
            is not null
        group by 1
    ),

    labels as (
        select outreach_id, model_purpose, model_confidence, read_purpose
        from {{ ref("outreach_purpose_labels") }}
    ),

    joined as (
        select
            s.*,
            coalesce(t.purpose, pb.purpose, so.purpose, ev.purpose) as picked_purpose,
            case
                when t.purpose is not null
                then 'turf'
                when pb.purpose is not null
                then 'call_list'
                when so.purpose is not null
                then 'social_post'
                when ev.purpose is not null
                then 'event'
            end as picked_from,
            case
                f.recommended_intent
                when 'introduce'
                then 'introduce_myself'
                when 'persuade'
                then 'persuade_voters'
                when 'event'
                then 'event_invite'
                when 'earlyVote'
                then 'early_voting'
                when 'electionDay'
                then 'election_day_turnout'
            end as recommended_purpose,
            f.is_recommended_modified,
            -- Keys from the template list as it stood before scripts held text.
            -- A key with no match in the purpose list stays null.
            case
                when
                    s.legacy_key in (
                        'smsPersuasive',
                        'robocallPersuasive',
                        'phoneBankingPersuasive',
                        'doorKnockingPersuasive'
                    )
                then 'persuade_voters'
                when
                    s.legacy_key in (
                        'smsGetOutTheVote',
                        'phoneBankingGetOutTheVote',
                        'doorKnockingGetOutTheVote',
                        'socialPostGetOutTheVote',
                        'smsElectionDay',
                        'robocallElectionDay',
                        'GOTV'
                    )
                then 'election_day_turnout'
                when s.legacy_key = 'smsEarlyVoting'
                then 'early_voting'
                when
                    s.legacy_key in (
                        'socialPostEventCalendar',
                        'communityEventInvitation',
                        'meetGreetEventAssets'
                    )
                then 'event_invite'
                when
                    s.legacy_key in (
                        'smsIntroduction',
                        'robocallIntroduction',
                        'launchSpeech',
                        'launchSocialMediaCopy',
                        'launchEmail'
                    )
                then 'introduce_myself'
                when
                    s.legacy_key in (
                        'candidateWebsite',
                        'socialMediaCopy',
                        'voterRegistrationDriveEmail',
                        'campaignUpdateEmail',
                        'pressRelease',
                        'generalInterviewPrep',
                        'fundraisingEmail'
                    )
                then 'other'
            end as template_purpose,
            -- A one-word script that is not a known key is a test entry.
            s.legacy_key is not null
            and s.legacy_key not in (
                'smsPersuasive',
                'robocallPersuasive',
                'phoneBankingPersuasive',
                'doorKnockingPersuasive',
                'smsGetOutTheVote',
                'phoneBankingGetOutTheVote',
                'doorKnockingGetOutTheVote',
                'socialPostGetOutTheVote',
                'smsElectionDay',
                'robocallElectionDay',
                'GOTV',
                'smsEarlyVoting',
                'socialPostEventCalendar',
                'communityEventInvitation',
                'meetGreetEventAssets',
                'smsIntroduction',
                'robocallIntroduction',
                'launchSpeech',
                'launchSocialMediaCopy',
                'launchEmail',
                'candidateWebsite',
                'socialMediaCopy',
                'voterRegistrationDriveEmail',
                'campaignUpdateEmail',
                'pressRelease',
                'generalInterviewPrep',
                'fundraisingEmail',
                'doorKnockingVoterId',
                'phoneBankingVoterId',
                'socialPostVoterId',
                'sms1MonthUntilElection',
                'robocall1MonthUntilElection',
                'socialPostTopIssues',
                'socialPostCommonQuestions',
                'doorKnockScript',
                'candidateTalkingPoints'
            ) as is_test,
            if(s.legacy_key is null, l.model_purpose, null) as model_purpose,
            if(s.legacy_key is null, l.model_confidence, null) as model_confidence,
            l.read_purpose
        from sends as s
        left join
            {{ ref("stg_airbyte_source__gp_api_db_door_knocking_turf") }} as t
            on t.id = s.door_knocking_turf_id
        left join
            {{ ref("stg_airbyte_source__gp_api_db_phone_banking_list") }} as pb
            on pb.id = s.phone_banking_list_id
        left join
            {{ ref("stg_airbyte_source__gp_api_db_outreach_social") }} as so
            on so.outreach_id = s.outreach_id
        left join
            {{ ref("stg_airbyte_source__gp_api_db_voter_file_filter") }} as f
            on f.voter_file_filter_id = s.voter_file_filter_id
        left join event_picks as ev on ev.outreach_id = s.outreach_id
        left join labels as l on l.outreach_id = s.outreach_id
    ),

    trusted as (
        select
            *,
            model_purpose is not null
            and (
                model_confidence = 'high'
                -- Hand readings of somewhat-confident Win guesses agreed on
                -- these purposes; persuasion was mostly an introduction.
                or (
                    product = 'win'
                    and model_confidence = 'medium'
                    and model_purpose
                    in ('introduce_myself', 'election_day_turnout', 'early_voting')
                )
            ) as is_model_trusted
        from joined
    )

select
    outreach_id,
    outreach_type,
    product,
    status,
    created_at,
    campaign_id,
    organization_slug,
    picked_purpose,
    picked_from,
    legacy_key,
    template_purpose,
    is_test,
    recommended_purpose,
    is_recommended_modified,
    model_purpose,
    model_confidence,
    read_purpose,
    coalesce(
        read_purpose,
        nullif(picked_purpose, 'custom'),
        template_purpose,
        recommended_purpose,
        if(is_model_trusted, model_purpose, null),
        picked_purpose,
        'unknown'
    ) as purpose,
    case
        when read_purpose is not null
        then 'reading'
        when nullif(picked_purpose, 'custom') is not null
        then 'picked'
        when template_purpose is not null
        then 'template'
        when recommended_purpose is not null
        then 'recommended'
        when is_model_trusted
        then 'model'
        when picked_purpose is not null
        then 'picked'
        else 'unknown'
    end as purpose_source,
    has_text
from trusted
