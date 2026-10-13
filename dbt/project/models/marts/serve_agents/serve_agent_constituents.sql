-- Serve product constituents: serve_agent_voters plus the people in the L2
-- commercial file who are not in the voter file, under the same de-identified,
-- non-partisan columns (those flagged is_available in l2_column_classification).
-- Consumer-only rows fill a column only where the commercial file has an
-- equivalent; turnout and Haystaq columns are null for them. LALVOTERID and the
-- commercial id are hashed to voter_key and never exposed.
-- depends_on: {{ ref("l2_column_classification") }}
{% set approved = l2_serve_available_columns() %}
select
    -- Voter rows get serve_agent_voters' key. The prefix keeps a commercial id
    -- from ever hashing to a voter's key, whatever the two id formats do.
    sha2(
        case
            when lalvoterid is not null
            then lalvoterid
            else concat('commercial:', individual_id)
        end,
        256
    ) as voter_key
    {%- for col in approved %}, `{{ col }}` {%- endfor %},
    is_registered_voter,
    district_source
from {{ ref("int__l2_nationwide_constituents_w_haystaq") }}
