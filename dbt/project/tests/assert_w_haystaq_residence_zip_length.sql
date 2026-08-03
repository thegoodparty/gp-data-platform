-- Guardrail: once populated, an L2 residence ZIP must be 5 characters. The
-- leading-zero states (NJ 08, MA 01, CT 06, NH 03, ME 04, RI 02, VT 05) silently
-- lose the leading zero the moment any layer types the ZIP as a number instead of
-- a string, turning 08731 into 8731. Stale rows in the incremental int layer have
-- carried a len-4 ZIP toward the serving DB before. A small number of source
-- values are genuinely malformed, so warn above that floor and error only on a
-- real regression: the smallest leading-zero state is ~468k rows, so any single
-- stripped state clears the error ceiling. Mailing_Addresses_Zip follows the same
-- rule; residence is the district-assignment key, so it is the one gated here.
{{ config(warn_if=">5000", error_if=">100000") }}

select state_postal_code, residence_addresses_zip as bad_zip
from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
where
    residence_addresses_zip is not null
    and residence_addresses_zip <> ''
    and length(residence_addresses_zip) <> 5
