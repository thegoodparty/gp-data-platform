# CLAUDE.md

Short, non-obvious context for `gp-data-matcha`. Full setup, run commands, and
auth live in `README.md`; this file covers what an agent needs that the code
and README do not already make obvious.

## What this is

Splink-based probabilistic entity resolution for cross-source record linkage
(candidacy stages, elected officials, election stages, people). The pipeline
is config-driven: each entity type's comparisons, blocking rules, EM training
blocks, and post-prediction filters live in a frozen `EntityConfig` in
`scripts/configs/`. Pipeline functions in `scripts/pipeline.py` take a `config`
parameter, so there is no hardcoded entity logic. The matcher reads a prematch
table (CSV or Databricks) and writes clustered + pairwise output. The person
lane also reads dbt's deterministic links (`--links`) and clusters over them
together with the scored pairs; see `pipeline.cluster_with_links`.
