-- Coherence check on the L2 intermediate layer. int__l2_nationwide_uniform_w_haystaq
-- inherits loaded_at from int__l2_nationwide_uniform, so after a coherent build the two
-- max(loaded_at) values must match. A mismatch means the layers are out of sync and
-- were
-- not built together, and EITHER side can be the stale one: the w_haystaq merge
-- lagging a
-- new uniform batch, or a regressed/partial uniform rebuild sitting behind an older,
-- still
-- coherent w_haystaq. So a red result is a signal to do a coherent full rebuild of
-- both,
-- not an instruction to rebuild w_haystaq (which can be the good side). Null-aware; the
-- is-null guard also fails an all-null loaded_at (both maxes null are "not distinct").
select *
from
    (
        select
            (
                select max(loaded_at) from {{ ref("int__l2_nationwide_uniform") }}
            ) as source_max_loaded_at,
            (
                select max(loaded_at)
                from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
            ) as int_max_loaded_at
    ) comparison
where
    source_max_loaded_at is distinct from int_max_loaded_at
    or source_max_loaded_at is null
