# CLAUDE.md

Short, non-obvious context for `gp-data-platform`. The repo overview is in `README.md`; dbt-specific guidance is in `dbt/project/CLAUDE.md`.

## General instructions
- Use terse comments that explain "why" not "what", and only when it's not obvious. Most comments should be a sentence or two at most
- Don't use the following phrases:
  - load-bearing
  - seam
  - substrate

## Subproject

Each subproject manages its own deps. `cd` into the right one before you install or run anything.

| Subproject | Tool | Python | Notes |
|---|---|---|---|
| `dbt/` | uv | 3.14 | `cd dbt && uv sync`, `uv run ...`. `dbt` itself is the system-installed dbt Cloud CLI; do not invoke it via uv. |
| `airflow/` | uv | 3.14 | Local DAG dev outside Astronomer (`cd airflow && uv sync`, `uv run pytest`). Deploy is Astro Runtime via `astro/Dockerfile` + `astro/requirements.txt` (not uv). To run Airflow itself: `cd airflow/astro && astro dev start`. |
| `analytics/` | uv | 3.14 | `cd analytics && uv sync`, `uv run ...`. |
| `matcha/` | uv | 3.14 | Splink entity-resolution pipeline. `cd matcha && uv sync`. Builds a container via `.github/workflows/matcha-container.yml`. |
| `gold-match/` | uv | 3.14 | L2-to-BallotReady district matcher, moved from omni @ `766137e50` and owned here — edit via normal PRs. `cd gold-match && uv sync`, `uv run pytest`. `bedrock_clients/` is the live model stack; the two Gemini modules in `shared/` are dormant until the evaluation gate passes, and `shared/` stays excluded from ruff (omni's inherited lint debt). |
| `reverse-etl/` | uv | 3.14 | Daily diff of a Databricks desired-state model against a destination (HubSpot contacts, CSV), keyed on a stable person id. `cd reverse-etl && uv sync`, `uv run ...`. Console script `retl`. Imports no Airflow code; the DAG passes credentials through the environment. |

Each subproject has its own CI workflow at `.github/workflows/<name>.yml`, path-filtered to its directory and running on its own Python (all on 3.14). There is no single root `pytest` job; tests are colocated under each directory (e.g. `airflow/astro/tests`, `dbt/tests`, `analytics/tests`).

## ai-rules submodule

`ai-rules/` is a git submodule (`thegoodparty/ai-rules`). After a fresh clone:

```bash
git submodule update --init --recursive
```

Don't edit files under `ai-rules/` directly. Changes belong in the submodule's upstream repo.

When reviewing changed code in this repo (e.g. during `/simplify`, `/review`, or any code-review pass), consult the rule files under `ai-rules/` in addition to this file and the per-subproject `CLAUDE.md` files.

## pre-commit

Driven by `pre-commit`:

- **Repo-wide lint/format** (ruff, ruff-format, sqlfmt, and the generic hooks) run on the default `pre-commit` stage and in CI (`pre-commit run --all-files` on every PR). A failing hook blocks the merge.
- **Per-directory tests** run on the `pre-push` stage only. Each directory has a `pytest-<dir>` hook gated by `files:`, so a push runs only the suites for the directories it touched, in that directory's own environment. These are local only: the `pre-push` stage keeps them out of the CI `pre-commit run --all-files` job, and CI test coverage is each directory's own workflow.

Install both hook types once (the config sets `default_install_hook_types`):

```bash
# from the repo root
pre-commit install
```

If `pre-commit` is not on your PATH, install it once with `pipx install pre-commit` (or `brew install pre-commit`).

For the per-directory test hooks to pass on push, set up the environment of each directory you touch: `uv sync` in `dbt/`, `airflow/`, `analytics/`, and `reverse-etl/`. Each hook `cd`s into its directory and runs the suite via that env (`uv run`), so you do not need to wrap `git` in any venv.

## The semantic layer is OKR metrics only

`dbt/project/models/**/sem_*.yml` is not a registry of every metric we compute. It holds the
numbers the business group actually rules on. Everything declared there enters the
ratification queue, so a metric nobody outside the data team reports on costs a real
person's attention for nothing.

**Before editing any `sem_*.yml`, say this to the person you are working with and wait for an
explicit yes:**

> This will change the semantic layer and notify people. Are you sure?

Its own question, answered on its own — not bundled into a list of others.

Why it has to be asked up front: a PR touching `sem_*.yml` posts a thread anchor to
#data-alignment that @-mentions both review groups, and `lanes.classify` routes an added
**or removed** metric to the business lane whether or not it was ever ratified. So a
mis-placed metric cannot be quietly withdrawn — the cleanup notifies too. There is no cheap
undo.

For a derived measure that is not an OKR, use a plain model column plus a self-contained
classifier macro (the shape of `amplitude_event_family`). "Should these two concepts stay
separate?" is a different question from "should the second become a governed metric"; do not
read approval of the first as approval of the second.

When changing the layer's **machinery** rather than a metric — seals, sidecar schema,
routing, the catalog generator — open the PR as a **draft**. `semantic-layer-thread.yml` is
draft-gated, so that keeps dev work out of a channel whose job is to announce metric changes.

## Never

- Don't add a root-level command that assumes one venv. State which subproject to `cd` into.
- Don't invoke `dbt` via `uv`. dbt Cloud CLI is system-installed.
- Don't disable pre-commit hooks to make a commit go through. CI runs `pre-commit run --all-files` and will catch a skipped lint/format hook.
- Don't commit secrets. `.env.example` is the only env file in git.
- Don't add a metric to a `sem_*.yml` without explicit confirmation. See above — it notifies both review groups and cannot be withdrawn quietly.
