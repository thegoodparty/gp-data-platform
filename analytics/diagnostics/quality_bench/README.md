# quality_bench — the analytics-framework quality benchmark

Measures whether the analytics-process + knowledge-skill framework produces
more correct, more consistent answers than bare Fable, and whether context
bloat is hurting. Design: `analytics/planning/2026-07-16-quality-benchmark-design.md`
(local). Tickets: DATA-2142 (parent), 2143 (harness), 2144 (bank), 2145 (verdict).

## Preflight

1. `databricks auth login` valid (floor_gen and all runs hit the warehouse).
2. `cd analytics && uv sync`.
3. `claude` CLI on PATH.

## Arm ref

`prep_arms.py` builds the `full` and `knowledge` arms as git worktrees checked
out from `--ref` (default `main`). Until this branch (`data-2143-quality-bench`)
merges, `main` lacks the Databricks deps it added, so worktrees cut from `main`
can't query the warehouse — pass `--ref data-2143-quality-bench` explicitly:

    uv run python diagnostics/quality_bench/prep_arms.py --ref data-2143-quality-bench

After the branch merges, the default (`main`) is correct and the flag can be
dropped.

## Invocation

Every CLI in this directory (`floor_gen.py`, `prep_arms.py`, `run_matrix.py`,
`grade.py`) bootstraps `sys.path` itself when imported as `__main__` (see the
`try/except ImportError` at the top of each module), so
`uv run python diagnostics/quality_bench/<tool>.py ...` works directly from
`analytics/` — no `PYTHONPATH` fiddling or `-m` invocation needed.

## Run a batch

    cd analytics
    uv run python diagnostics/quality_bench/floor_gen.py         # refresh floor.md
    uv run python diagnostics/quality_bench/prep_arms.py          # build 3 arm dirs
    uv run python diagnostics/quality_bench/run_matrix.py --batch 2026-07-20 --reps 3
    uv run python diagnostics/quality_bench/grade.py --batch 2026-07-20 --judge

Outputs: `results/<batch>/scores.csv`, `results/<batch>/report.md` (gitignored).
Resume: rerunning run_matrix with the same --batch skips completed runs.

The real-batch default model is `claude-fable-5` (both `run_matrix.py` and
`grade.py --judge` default to it). A smoke run can use a cheaper `--model`
(e.g. `sonnet`) since it only proves the pipes work, not framework quality.

## Verdict rules (pre-registered, design §8 — do not renegotiate post hoc)

1. **Trustworthy:** full arm passes ≥7/8 questions in ≥2/3 reps, zero
   severity-1 anywhere in full-arm runs, all full-arm cells consistent.
2. **Earns overhead:** full beats bare on trap questions.
3. **Bloat signal:** knowledge-only ≥ full ⇒ prune proposals (with token
   deltas; deletion is the default direction).

## Holdout rule

Questions tagged `split: holdout` in the manifest must NEVER be cited in any
skill fix or calibration pass. Promoting a holdout failure into a skill edit
burns the question: retag it `calibration` and author a replacement holdout.

## Key authoring (DATA-2144)

Gold-run + review per key: one supervised full-framework run with Tristan
challenging scoping; Tristan corrects before the key is pinned. Jot the
intent card during review (3-4 lines). Verify the population-overlap key with
direct SQL, never a skill's claim. Format: `keys/KEY_SCHEMA.md`.

## Judge audit

With --judge, hand-review ~10% of judge grades, prioritizing runs where the
judge and deterministic layers disagree. Judge scores never override
deterministic failures.

Known limitation: `judge.parse_judge_output` takes the *last* fenced ```yaml
block in the judge's output that parses as a `judge:` verdict (see the
docstring in `judge.py`). If the graded answer itself contains an echoed or
example ```yaml fence that happens to parse as a `judge:` block, it could
shadow the real verdict fence emitted after it. Not observed in practice, but
worth checking first if a judge score looks nonsensical.

## Smoke test (q00_smoke)

`questions/q00_smoke.md` + `keys/q00_smoke_key.yaml` is a trivial live
question ("how many tables in the `dbt` schema") that any arm should answer
correctly — it exists to prove the pipes work end to end, not to measure
framework quality. Run it with a cheap model and only 2 arms:

    cd analytics
    uv run python diagnostics/quality_bench/floor_gen.py
    uv run python diagnostics/quality_bench/prep_arms.py --ref data-2143-quality-bench
    uv run python diagnostics/quality_bench/run_matrix.py --batch smoke --reps 1 --arms bare full --model sonnet
    uv run python diagnostics/quality_bench/grade.py --batch smoke

**Rep-threshold note:** verdict rule 1 (`rule1_trustworthy`) requires passing
≥2/3 reps, and cell consistency (`grading.cell_consistency`) needs ≥2 parsed
reps to say anything about spread. With `--reps 1` the smoke can never satisfy
either — that's expected, not a bug. The smoke's actual success criterion is:

- both runs (`bare`, `full`) have `ok=true` in `state.json`,
- `results/smoke/scores.csv` has 2 rows, both `numbers_pass=True`,
- `results/smoke/report.md` renders with the verdict/cell fields present
  (values will show `None`/inconclusive because of the single rep — that's
  correct, not a failure).

Debug notes: if a run fails, read the copied transcript in `results/smoke/`;
if the transcript is missing, check the munge in `transcript_path` against the
actual dir name under `~/.claude/projects/`.
