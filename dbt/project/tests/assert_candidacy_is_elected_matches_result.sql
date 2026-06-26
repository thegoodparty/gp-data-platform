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
        -- Everything that is neither the TRUE value nor a FALSE value must be
        -- NULL. Expressed as the complement of the two sets above (rather than
        -- enumerating the undecided values) so a newly added candidacy_result
        -- value can't silently slip through with a non-NULL is_elected.
        coalesce(candidacy_result, '__null__') not in (
            'Won',
            'Lost',
            'Lost Primary',
            'Lost Primary Runoff',
            'Withdrew',
            'Not on Ballot'
        )
        and is_elected is not null
    )
