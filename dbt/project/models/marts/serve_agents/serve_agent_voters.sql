-- serve_agent_voters: de-identified, non-partisan voter dataset for the Serve
-- product's AI agents (DATA-2003, TDD DATA-1977). One row per voter, a proxy for
-- a constituent.
--
-- The exposed projection is governed by the serve_agent_voters_columns seed: this
-- model reads it at build time and selects exactly the columns flagged
-- is_available = true. To add or retract a field, flip is_available in the seed
-- and rebuild; no change to this SQL is needed. PII/direct identifiers and all
-- partisanship are excluded by that contract. The raw LALVOTERID is never exposed;
-- it is replaced by a one-way SHA-256 voter_key so constituents can be counted and
-- joined without surfacing the L2 identifier.
--
-- Materialized as a view (configured in dbt_project.yml) over the already-built
-- int__l2_nationwide_uniform_w_haystaq table.
-- depends_on: {{ ref("serve_agent_voters_columns") }}
{% set approved = [] %}
{% if execute %}
    {% set results = run_query(
        "select column_name from "
        ~ ref("serve_agent_voters_columns")
        ~ " where lower(cast(is_available as string)) = 'true' order by column_name"
    ) %}
    {% set approved = results.columns[0].values() %}
{% endif %}
select
    sha2(lalvoterid, 256) as voter_key
    {%- for col in approved %}, `{{ col }}` {%- endfor %}
from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
