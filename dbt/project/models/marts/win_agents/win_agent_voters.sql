-- Win product voter file: mirrors serve_agent_voters but removes PII only,
-- retaining partisan and commercial fields. Projection is the classification
-- seed minus the identity_pii and operational families. LALVOTERID is hashed to
-- voter_key (identical to serve) and never exposed.
-- depends_on: {{ ref("l2_column_classification") }}
{% set exposed = l2_columns_excluding_families(["identity_pii", "operational"]) %}
select
    sha2(lalvoterid, 256) as voter_key
    {%- for col in exposed %}, `{{ col }}` {%- endfor %}
from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
