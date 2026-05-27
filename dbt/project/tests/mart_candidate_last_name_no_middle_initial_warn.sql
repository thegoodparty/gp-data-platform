-- Mart-level last_name pollution check (source-agnostic, warn-only).
--
-- Catches mart rows where last_name has middle-initial pollution
-- regardless of which source contributed it. Source-agnostic on
-- purpose: the TS-parser-specific test lives at
-- tests/mart_candidate_techspeed_last_name_no_middle_initial.sql
-- (despite the filename, scoped to int__civics_candidate_techspeed
-- after DATA-1523), and this mart-level test surfaces cross-source
-- consolidation issues that the parser-specific test can't see.
--
-- Excludes hubspot-only rows (the 2025 legacy archive is addressed by
-- a separate ticket and intentionally not in scope here). Includes any
-- row whose source_systems contains at least one non-hubspot source.
--
-- Known cases at PR-418 time:
-- - Paul Price (IL): BR + DDHQ + TS all contribute 'Price', gp_api
-- contributes 'S Price', mart precedence picks the polluted value.
-- Separate ticket should investigate mart last_name precedence
-- when sources disagree.
--
-- Warn-only on purpose: the underlying data quality belongs to the
-- contributing sources / mart consolidation logic, not to this PR.
-- If the count grows materially (e.g. > 50), revisit.
{{ config(severity="warn") }}

select gp_candidate_id, last_name, source_systems
from {{ ref("candidate") }}
where
    last_name is not null
    and not (size(source_systems) = 1 and array_contains(source_systems, 'hubspot'))
    and (
        last_name rlike '^[A-Z][.] ?[A-Za-z]'
        or last_name rlike '^[A-Z] [A-Za-z]'
        or last_name rlike '[A-Za-z] [A-Z]$'
        or last_name rlike ',$'
    )
