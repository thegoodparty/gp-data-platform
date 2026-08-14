with
    source as (
        select * from {{ source("model_predictions", "web_verify_verdicts_20260813") }}
    ),
    renamed as (
        select
            verify_id,
            campaign_id,
            user_id,
            wave,
            method_version,
            verdict,
            -- Means *admissible* evidence, and a null means two different things
            -- depending on the wave. The flag below keeps them separable: early
            -- waves recorded no url at all, while in an evidenced wave a null
            -- url is exactly a not-found verdict.
            evidence_url,
            source_type,
            wave in ('sweep', 'mopup') as is_evidence_recorded,
            notes,
            -- Set only where a verdict or url was changed after the run. Null
            -- means the verifier's call stands as recorded.
            adjudication_note,
            verified_at,
            -- Sampling metadata, not row properties. The waves are separate
            -- frames, so a rate pooled across them is a composition artifact;
            -- these are what a consumer conditions on to avoid that.
            oof_hgb,
            is_cleanlab_flagged,
            is_high_confidence_flag,
            score_bin,
            bin_population
        from source
    )
select *
from renamed
