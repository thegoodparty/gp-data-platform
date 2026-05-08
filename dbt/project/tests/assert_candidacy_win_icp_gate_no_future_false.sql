-- WIN-ICP gate contract: is_win_icp / is_win_supersize_icp may only be forced
-- to false when *every* election date is null or strictly before the office's
-- icp_win_effective_date. Any future election date should bypass the gate and
-- let icp_office_win / icp_win_supersize flow through unchanged.
--
-- This test isolates "gate forced false" from "underlying ICP attribute is
-- itself false" by checking the underlying icp.* attribute is true. Zero rows
-- expected.
select
    c.gp_candidacy_id,
    c.is_win_icp,
    c.is_win_supersize_icp,
    c.primary_election_date,
    c.primary_runoff_election_date,
    c.general_election_date,
    c.general_runoff_election_date,
    icp.icp_win_effective_date,
    icp.icp_office_win,
    icp.icp_win_supersize
from {{ ref("candidacy") }} as c
join
    {{ ref("int__icp_offices") }} as icp
    on c.br_position_database_id = icp.br_database_position_id
where
    icp.icp_win_effective_date is not null
    and (
        c.primary_election_date >= icp.icp_win_effective_date
        or c.primary_runoff_election_date >= icp.icp_win_effective_date
        or c.general_election_date >= icp.icp_win_effective_date
        or c.general_runoff_election_date >= icp.icp_win_effective_date
    )
    and (
        (c.is_win_icp = false and icp.icp_office_win = true)
        or (c.is_win_supersize_icp = false and icp.icp_win_supersize = true)
    )
