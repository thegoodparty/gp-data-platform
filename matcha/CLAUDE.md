# CLAUDE.md

Short, non-obvious context for `gp-data-matcha`. Full setup, run commands, and
auth live in `README.md`; this file covers what an agent needs that the code
and README do not already make obvious.

## What this is

Splink-based probabilistic entity resolution for cross-source record linkage
(candidacy stages, elected officials, election stages). The pipeline is
config-driven: each entity type's comparisons, blocking rules, EM training
blocks, and post-prediction filters live in a frozen `EntityConfig` in
`scripts/configs/`. Pipeline functions in `scripts/pipeline.py` take a `config`
parameter, so there is no hardcoded entity logic. The matcher reads a prematch
table (CSV or Databricks) and writes clustered + pairwise output.

## ai-rules submodule

`ai-rules/` is a git submodule (`thegoodparty/ai-rules`). It carries the
org-wide AI context layer (system map, repo index, cross-repo flows, templates)
and the review-time critics. After a fresh clone:

```bash
git submodule update --init --recursive
```

Do not edit files under `ai-rules/` directly. Changes belong in the submodule's
upstream repo.

When reviewing changed code in this repo (for example during `/simplify`,
`/review`, or any code-review pass), consult the rule files under `ai-rules/`
alongside this file.
