# Genie Tools

Small export-only utilities for checking Databricks Genie space config into git.

The primary workflow is:

1. Make changes in the Genie UI.
2. Export the space config into this repo.
3. Review the normalized diff.
4. Use the normalized or redacted JSON as LLM context.

This first iteration is intentionally export-only. It does not apply or update Genie spaces.

## Auth

The CLI uses the standard Databricks Python SDK auth flow through `WorkspaceClient()`.

That means it will use your existing Databricks auth configuration, for example:

- `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
- `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`
- `DATABRICKS_CONFIG_PROFILE`

No custom auth handling is built into this tool.

In this repo, the most reliable way to run the tool is to use the existing Poetry
environment from `dbt/project` and a Databricks CLI profile stored in
`~/.databrickscfg`.

Example auth checks:

```bash
databricks auth profiles
databricks auth login --profile <PROFILE> --host <DATABRICKS_HOST>
```

If a profile exists but shows `Valid NO`, refresh it with `databricks auth login`
before running exports.

## Install

From the repo root:

```bash
python3 -m pip install -e apps/genie-tools
```

For this repo, an already-working option is:

```bash
export REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT/dbt/project"
eval "$(poetry env activate)"
export PYTHONPATH="$REPO_ROOT/apps/genie-tools/src"
```

That reuses the existing Poetry environment, which already includes `databricks-sdk`.

## Local Env File

The repo root already ignores `.env` and `.env.local`, so a repo-local env file is a
safe place for non-secret defaults such as:

- `DATABRICKS_CONFIG_PROFILE`
- `DATABRICKS_HOST`
- `GENIE_TOOLS_SPACE_ID`
- `GENIE_TOOLS_OUT_DIR`

Example:

```bash
cat > apps/genie-tools/.env.local <<'EOF'
DATABRICKS_CONFIG_PROFILE=<PROFILE>
DATABRICKS_HOST=https://dbc-<workspace>.cloud.databricks.com
GENIE_TOOLS_SPACE_ID=<SPACE_ID>
GENIE_TOOLS_OUT_DIR=genie/civics/spaces/prod
EOF

set -a
source apps/genie-tools/.env.local
set +a
```

Keep secrets out of this file if you already authenticate through `~/.databrickscfg`.

## Commands

Export a Genie space:

```bash
python -m genie_tools.cli export \
  --space-id <SPACE_ID> \
  --out-dir genie/civics/spaces/prod \
  --write-redacted \
  --write-metadata
```

Validate a normalized export:

```bash
python -m genie_tools.cli validate \
  --file genie/civics/spaces/prod/normalized/<SPACE_ID>.json
```

Using the local env file:

```bash
export REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT/dbt/project"
eval "$(poetry env activate)"
set -a
source "$REPO_ROOT/apps/genie-tools/.env.local"
set +a
export PYTHONPATH="$REPO_ROOT/apps/genie-tools/src"

python -m genie_tools.cli export \
  --space-id "$GENIE_TOOLS_SPACE_ID" \
  --out-dir "$REPO_ROOT/$GENIE_TOOLS_OUT_DIR" \
  --write-redacted \
  --write-metadata

python -m genie_tools.cli validate \
  --file "$REPO_ROOT/$GENIE_TOOLS_OUT_DIR/normalized/$GENIE_TOOLS_SPACE_ID.json"
```

## Output Files

- `raw/<SPACE_ID>.json`: exact parsed `serialized_space` export from Databricks
- `normalized/<SPACE_ID>.json`: diff-friendly JSON with recursively sorted object keys
- `redacted/<SPACE_ID>.json`: optional LLM-friendly artifact with lightweight redaction for workspace-specific values
- `metadata/<SPACE_ID>.json`: optional export metadata such as export time, workspace host, and selected space attributes when available

Recommended check-in target:

- commit `normalized/<SPACE_ID>.json`
- treat `raw/`, `redacted/`, and `metadata/` as local byproducts unless your team explicitly decides otherwise

## Redaction

`--write-redacted` produces a normalized copy with lightweight redaction.

The first pass is intentionally simple:

- key-aware placeholders for obvious environment-specific fields such as workspace IDs, warehouse IDs, space IDs, hosts, URLs, and absolute paths
- deterministic replacement of opaque Genie object IDs like sample question IDs and benchmark IDs
- replacement of catalog/schema prefixes while preserving table names and SQL intent
- Databricks URL and path regex cleanup
- optional caller-provided string replacements through `GENIE_TOOLS_REDACTIONS_JSON`

Example:

```bash
export GENIE_TOOLS_REDACTIONS_JSON='{"catalog_prod":"<CATALOG>","schema_prod":"<SCHEMA>"}'
```

The goal is to preserve semantic content such as instructions, joins, SQL, examples, and benchmarks while removing obvious environment-specific tokens.

## Validation

Validation is intentionally lightweight. It checks that:

- the file is valid JSON
- the top-level value is an object
- the object is not empty
- the export contains structured Genie config content

This is meant to catch obviously bad exports, not fully validate Genie semantics.

For the live export tested on March 21, 2026, the top-level structure was:

- `version`
- `config`
- `data_sources`
- `instructions`
- `benchmarks`

Within that payload we saw:

- `config.sample_questions`
- `data_sources.tables`
- `instructions.example_question_sqls`
- `instructions.join_specs`
- `instructions.sql_snippets`
- `instructions.text_instructions`
- `benchmarks.questions`

Known limitation from the live export:

- `serialized_space` did not include most table descriptions or column descriptions that were visible in the UI
- in the tested export, only `1` table description and `3` column descriptions were present across `169` columns
- if descriptions matter, compare the export against the Genie UI before treating it as complete

## Future Work

- apply or import support
- CI drift detection
- richer schema-aware validation
- focused diff helpers for review workflows
