# 🍵 Matcha: Multi-Source Entity Resolution

Splink-based probabilistic record linkage for cross-source entity
resolution. Supports candidacy stages and elected officials matching.

- **Candidacy** — matches candidacy stage records (BR, TS, DDHQ) by person + office + election date
- **Elected Official** — matches elected official records (BR, TS) by person + office (no election date)
- **Election Stage** — matches race/contest records (BR, DDHQ, TS) by office + election date + jurisdiction (no person fields)

The pipeline supports any number of sources discovered dynamically from the
`source_name` column. Entity-specific configuration (comparisons, blocking
rules, filters) lives in `scripts/configs/`.

## Quick start

### Prerequisites

- Python 3.11+ and [uv](https://docs.astral.sh/uv/) for local development
- Docker for container-based runs
- [GitHub CLI](https://cli.github.com/) (`gh`) for pulling pre-built images

### pre-commit

CI runs `ruff check` and `ruff format --check` on every PR, so a lint or format failure will block the lint job. The monorepo's root `.pre-commit-config.yaml` runs the same ruff hooks repo-wide. Install the git hook once from the monorepo root so it runs before you push:

```bash
# from the monorepo root, one-time
pre-commit install
```

If `pre-commit` is not on your PATH, install it once with `pipx install pre-commit` (or `brew install pre-commit`).

### Local (uv)

```bash
uv sync

# Candidacy matching (default)
uv run python -m scripts.cli match --entity-type candidacy_stage --input input.csv

# Elected official matching
uv run python -m scripts.cli match --entity-type elected_official --input input.csv

# Election stage matching
uv run python -m scripts.cli match --entity-type election_stage --input input.csv
```

### Docker (pre-built image)

Authenticate Docker with GHCR using the GitHub CLI:

```bash
# One-time: ensure gh has the packages scope
gh auth refresh -s read:packages

# Log Docker into GHCR
echo $(gh auth token) | docker login ghcr.io -u $(gh api user --jq .login) --password-stdin
```

Pull and run the latest image:

```bash
docker pull ghcr.io/thegoodparty/gp-data-matcha:latest

# Show help
docker run ghcr.io/thegoodparty/gp-data-matcha:latest match --help

# Run with a local CSV
docker run \
  -m 8g --cpus 4 \
  -v ~/path/to/input.csv:/app/data/input.csv \
  ghcr.io/thegoodparty/gp-data-matcha:latest \
  match --input /app/data/input.csv --output-dir /app/out
```

PR builds are tagged `pr-<number>` (e.g. `ghcr.io/thegoodparty/gp-data-matcha:pr-2`).

### Docker (local build)

```bash
docker build -t gp-data-matcha .
docker run gp-data-matcha match --help
```

### Databricks input + output

```bash
export DATABRICKS_HOST=dbc-abc123.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcdef1234567890
export DATABRICKS_CLIENT_ID=<service-principal-client-id>
export DATABRICKS_CLIENT_SECRET=<service-principal-secret>

uv run python -m scripts.cli match \
  --entity-type candidacy_stage \
  --input goodparty_data_catalog.dbt.int__er_prematch_candidacy_stages \
  --output-cluster-table goodparty_data_catalog.dbt.er_clustered_candidacies \
  --output-pairwise-table goodparty_data_catalog.dbt.er_pairwise_predictions \
  --overwrite

# Elected officials
uv run python -m scripts.cli match \
  --entity-type elected_official \
  --input goodparty_data_catalog.dbt.int__er_prematch_elected_officials \
  --output-cluster-table goodparty_data_catalog.er_source.er_clustered_elected_officials \
  --output-pairwise-table goodparty_data_catalog.er_source.er_pairwise_elected_officials \
  --overwrite

# Election stages
uv run python -m scripts.cli match \
  --entity-type election_stage \
  --input goodparty_data_catalog.dbt.int__er_prematch_election_stages \
  --output-cluster-table goodparty_data_catalog.er_source.er_clustered_election_stages \
  --output-pairwise-table goodparty_data_catalog.er_source.er_pairwise_election_stages \
  --overwrite
```

## CLI reference

```
Usage: cli.py match [OPTIONS]

Options:
  --entity-type [candidacy_stage|elected_official|election_stage]  Entity type to match (default: candidacy_stage).
  --input TEXT                  Path to prematch CSV or Databricks FQN (catalog.schema.table). Required.
  --output-dir DIRECTORY        Directory for local results. Default: results/<entity-type>/
  --output-cluster-table TEXT   Databricks FQN to upload clustered results (catalog.schema.table).
  --output-pairwise-table TEXT  Databricks FQN to upload pairwise predictions (catalog.schema.table).
  --overwrite                   Overwrite existing Databricks output tables.
```

### Audit subcommands

```bash
uv run python -m scripts.cli audit summary --entity-type candidacy_stage --results-dir results/candidacy_stage/
uv run python -m scripts.cli audit low-confidence --entity-type candidacy_stage --results-dir results/candidacy_stage/ --sample 20
uv run python -m scripts.cli audit false-negatives --entity-type elected_official --results-dir results/elected_official/
```

When `--run-audit` is enabled (default), all three audits run automatically after
matching and log results. The standalone commands above are available for manual
re-runs against saved CSV results.

### Auditing with Claude Code

The `/audit-er-results` skill (`.claude/skills/audit-er-results/skill.md`)
provides a guided workflow for auditing match quality with Claude Code. It
runs through summary stats, low-confidence pair review, false negative
analysis, regression checking, and produces actionable recommendations. Use it
after a match run or when evaluating changes to blocking rules, comparisons,
or post-prediction filters:

```
/audit-er-results
```

**Input:** CSV file or Databricks table from `int__er_prematch_candidacy_stages` or `int__er_prematch_elected_officials`
**Output** (in `results/<entity-type>/`):
- `pairwise_predictions.csv` — all scored candidate pairs
- `clustered_candidacies.csv` or `clustered_elected_officials.csv` — all records with cluster assignments
- `match_weights_chart.html` — Splink match weight visualization
- `m_u_parameters_chart.html` — learned m/u probability visualization
- `audit_summary.csv` — match coverage stats per source
- `audit_low_confidence.csv` — most ambiguous pairs for review
- `audit_false_negatives.csv` — plausible matches the model missed

## Authentication

Two auth modes are supported, resolved automatically:

### Local development (Databricks CLI)

If you've authenticated with the Databricks CLI (`databricks configure` or
`databricks auth login`), the pipeline picks up your profile automatically.
Only one env var is needed:

```bash
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcdef1234567890
```

`DATABRICKS_HOST` is optional — it will be read from your CLI profile if not set.

### Production (OAuth M2M service principal)

Used in Docker containers and Airflow. All four env vars are required:

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Databricks workspace hostname |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path |
| `DATABRICKS_CLIENT_ID` | Service principal client ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal secret |

The pipeline detects which mode to use based on whether `DATABRICKS_CLIENT_ID`
and `DATABRICKS_CLIENT_SECRET` are set.

## Architecture

The pipeline is **config-driven**. All entity-specific settings (comparisons,
blocking rules, EM training blocks, post-prediction filters, audit columns)
live in frozen `EntityConfig` dataclasses in `scripts/configs/`. Pipeline
functions accept a `config` parameter — no hardcoded entity logic.

```
scripts/
  entity_config.py          # EntityConfig dataclass + get_config() registry
  constants.py              # OFFICE_STOP_WORDS shared by both configs
  configs/
    candidacy.py            # CANDIDACY_CONFIG
    elected_official.py     # ELECTED_OFFICIAL_CONFIG
  pipeline.py               # All functions accept config param
  cli.py                    # --entity-type routes to correct config
```

## How it works

The pipeline uses [Splink 4](https://moj-analytical-services.github.io/splink/)
in `link_only` mode (cross-source matching, no within-source dedup) with
DuckDB as the backend. Sources are discovered dynamically from
`df["source_name"].unique()` and passed as a list to the Splink `Linker`.

### Preprocessing

Most data cleaning is handled upstream in the dbt prematch models. The
Python script performs only:

- **First name nicknames:** parses JSON alias arrays so Splink's
  `ArrayIntersectLevel` can check for overlap (e.g. "robert" and "bob")
- **Date parsing:** converts date columns (e.g. `election_date` for candidacy)
  to consistent format. Controlled by `config.date_columns`.
- **Nulls:** literal `"null"` strings, empty strings, and `NaN` are all
  converted to `None` so Splink treats them as missing data

### Entity type comparison

| Aspect | Candidacy | Elected Official |
|--------|-----------|-----------------|
| Input table | `int__er_prematch_candidacy_stages` | `int__er_prematch_elected_officials` |
| Comparisons | 9 (incl. `election_date`) | 10 (incl. `office_type`, `office_level`) |
| Blocking rules | 6 (incl. `br_race_id`) | 5 (no race ID) |
| Date columns | `election_date` | none |
| EM training | last_name+state+election_date, first_name, email, state+election_date+last_name | last_name+state+office_level, first_name+state, phone, state+office_type+last_name |
| Post-prediction filter | Person + office + race ID guard | Person + office (no race ID guard) |
| Cluster grain | Candidacy (person+office+election) | Person (multi-term records cluster together) |
| Thresholds | predict=0.01, cluster=0.95 | predict=0.01, cluster=0.95 |

Both entity types share:
- Person-level comparisons: `last_name` (JW with TF adjustment), `first_name`
  (custom with nickname aliases), `party`, `email`, `phone`
- Office-level comparisons: `state`, `official_office_name` (JW with 0.75 tier),
  `district_identifier`
- Post-prediction person identity filter: `gamma_last_name > 0 AND
  (gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0)`
- Office name locality-token fallback (shared `OFFICE_STOP_WORDS` list)

For full config details, see `scripts/configs/candidacy.py` and
`scripts/configs/elected_official.py`.

## Testing

### Unit tests

```bash
uv run pytest tests/ -v
```

### Integration tests (requires Databricks)

Integration tests run the full CLI pipeline against a live Databricks SQL
warehouse. They are automatically skipped when `DATABRICKS_HTTP_PATH` is not
set.

```bash
# Local dev (Databricks CLI auth)
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcdef1234567890

# CI / production (OAuth M2M)
export DATABRICKS_HOST=dbc-abc123.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcdef1234567890
export DATABRICKS_CLIENT_ID=<service-principal-client-id>
export DATABRICKS_CLIENT_SECRET=<service-principal-secret>

uv run pytest tests/ -m integration -v
```

The integration fixture creates an ephemeral Databricks schema, uploads the
dummy test data, runs the match pipeline, and tears down the schema on
completion.

## CI/CD

The monorepo's `.github/workflows/matcha-container.yml` workflow builds multi-arch (amd64 + arm64) images (it fires only on changes under `matcha/`):
- **On PR:** builds and pushes `ghcr.io/thegoodparty/gp-data-matcha:pr-<number>`
- **On push to main:** pushes `:latest` and `:<sha>` tags
- **On PR close:** cleans up the PR-specific image tag
