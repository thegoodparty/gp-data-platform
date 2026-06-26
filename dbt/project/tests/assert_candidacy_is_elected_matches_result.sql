-- is_elected must be a faithful function of candidacy_result:
-- TRUE  iff candidacy_result = 'Won' (won the seat / will serve)
-- FALSE iff candidacy_result is a terminal non-win
-- NULL  otherwise (still undecided, or no result captured)
-- Guards against the column drifting from candidacy_result (e.g. a lateral
-- reference resolving to the wrong source column). Zero rows expected.
select gp_candidacy_id, candidacy_result, is_elected
from {{ ref("candidacy") }}
where
    (candidacy_result = 'Won' and is_elected is not true)
    or (
        candidacy_result
        in ('Lost', 'Lost Primary', 'Lost Primary Runoff', 'Withdrew', 'Not on Ballot')
        and is_elected is not false
    )
    or (
        (
            candidacy_result is null
            or candidacy_result in (
                'Won Primary',
                'Won Primary Runoff',
                'Runoff',
                'Runoff Primary',
                'Cannot Determine'
            )
        )
        and is_elected is not null
    )
