-- Staging keeps a phone value only when its digits form a 10-digit US number.
-- A violation here means the sanitizer regressed or a state's rows reached
-- this table through some path that bypasses the staging views.
select `VoterTelephones_CellPhoneFormatted` as phone, state_postal_code
from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
where
    `VoterTelephones_CellPhoneFormatted` is not null
    and length(regexp_replace(`VoterTelephones_CellPhoneFormatted`, '[^0-9]', '')) != 10

union all

select `VoterTelephones_LandlineFormatted` as phone, state_postal_code
from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
where
    `VoterTelephones_LandlineFormatted` is not null
    and length(regexp_replace(`VoterTelephones_LandlineFormatted`, '[^0-9]', '')) != 10
