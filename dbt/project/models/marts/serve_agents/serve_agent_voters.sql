-- Serve product voter file: de-identified, non-partisan (DATA-2003). Exposes
-- only columns flagged is_available in the l2_column_classification seed, so
-- PII, partisan, and commercial fields are excluded. LALVOTERID is hashed to
-- voter_key and never exposed.
-- depends_on: {{ ref("l2_column_classification") }}
{% set approved = l2_serve_available_columns() %}
select
    sha2(lalvoterid, 256) as voter_key
    {%- for col in approved %}, `{{ col }}` {%- endfor %}
from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
