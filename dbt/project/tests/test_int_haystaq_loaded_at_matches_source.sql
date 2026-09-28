-- Coherence check on the L2 intermediate layer, per state.
-- int__l2_nationwide_uniform_w_haystaq inherits loaded_at from
-- int__l2_nationwide_uniform, so after its incremental merge each state's
-- max(loaded_at) equals the uniform side as of the moment it read it. The two
-- sides are rebuilt by different jobs (the uniform base merges on every nightly
-- run, w_haystaq monthly), so a load landing between w_haystaq's read and this
-- test is ordinary, not a fault, and an exact match cannot be required.
--
-- Two things are faults. The uniform side sitting behind w_haystaq for a state
-- is a regressed or partial uniform rebuild; w_haystaq is the good side there
-- and must not be "fixed" toward it. A load that w_haystaq has still not
-- merged after a full rebuild cycle means its watermark missed it. The age is
-- measured from the load to now, not between the two loaded_at values: the
-- gap between consecutive deliveries says nothing about how long the newer
-- one has waited, and deliveries can be further apart than the bound.
--
-- The age bound is the cadence of the monthly w_haystaq rebuild plus slack,
-- not a property of the data; it lets the test run on any day of the cycle.
{% set max_unmerged_days = 35 %}

with
    uniform as (
        select state_postal_code, max(loaded_at) as uniform_max_loaded_at
        from {{ ref("int__l2_nationwide_uniform") }}
        group by state_postal_code
    ),

    w_haystaq as (
        select state_postal_code, max(loaded_at) as w_haystaq_max_loaded_at
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
        group by state_postal_code
    ),

    compared as (
        select
            coalesce(
                uniform.state_postal_code, w_haystaq.state_postal_code
            ) as state_postal_code,
            uniform.uniform_max_loaded_at,
            w_haystaq.w_haystaq_max_loaded_at,
            case
                when uniform.uniform_max_loaded_at is null
                then 'uniform_missing_or_null'
                when w_haystaq.w_haystaq_max_loaded_at is null
                then 'w_haystaq_missing_or_null'
                when w_haystaq.w_haystaq_max_loaded_at > uniform.uniform_max_loaded_at
                then 'uniform_behind_w_haystaq'
                when
                    uniform.uniform_max_loaded_at > w_haystaq.w_haystaq_max_loaded_at
                    and uniform.uniform_max_loaded_at
                    < current_timestamp() - interval {{ max_unmerged_days }} days
                then 'w_haystaq_stale'
            end as failure
        from uniform
        full outer join
            w_haystaq on uniform.state_postal_code = w_haystaq.state_postal_code
    )

select *
from compared
where failure is not null
