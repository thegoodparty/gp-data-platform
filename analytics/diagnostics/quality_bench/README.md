# quality_bench — the analytics-framework quality benchmark

Measures whether the analytics-process + knowledge-skill framework produces
more correct, more consistent answers than bare Fable, and whether context
bloat is hurting. Design: `analytics/planning/2026-07-16-quality-benchmark-design.md`
(local). Tickets: DATA-2142 (parent), 2143 (harness), 2144 (bank), 2145 (verdict).

## Preflight

1. `databricks auth login` valid (floor_gen and all runs hit the warehouse).
2. `cd analytics && uv sync`.
3. `claude` CLI on PATH.

## Pre-first-batch checklist

Two live probes to run once before the first real batch — cheap, and they catch
the two failure modes the unit tests can't:

1. **Judge path.** Run `grade.py --judge` on the smoke batch and confirm the
   report shows `Judged: X/X` (no `judge FAILED` lines). This exercises the
   judge's `--tools ""` invocation end to end; if it fails, the judged layer
   silently drops on every real batch.
2. **Full-arm headless tools.** Do one full-arm headless run with a prompt that
   engages the analytics-process skill (e.g. a real product question), and
   confirm the transcript shows the `Skill`/`Agent` tools firing rather than a
   permission denial. Headless `claude -p` only allows what
   `.claude/settings.local.json` lists, so if either is denied, add it to
   `SETTINGS_JSON` in `prep_arms.py` and re-prep the arms. (Dead relative
   links are no longer a runtime concern to watch for here — `prep_arms.py`'s
   integrity gate now fails the prep step outright if any md link inside an
   arm doesn't resolve, so a prepped arm can't reach a run with one.)

Both probes passed 2026-07-24: judge on the smoke batch graded 2/2 with no
failures, and a full-arm headless run of q01 (batch `probe-skill`) invoked
analytics-process + win-analytics-knowledge with zero permission denials.

## No-decision gates

**The current harness must not produce decision-bearing verdicts.** Calibration
batches and probes are fine; acting on a rule 1/2/3 verdict (pruning context,
declaring the framework trustworthy or not) is not, until every gate below is
closed:

1. Both pre-first-batch probes above have passed.
2. Isolation is a permission boundary, not a filesystem one — runs execute as
   the operator's user and could in principle read the original checkout
   (answer keys) or sibling runs. Gate: container-grade isolation
   (ClickUp 86ajmyk2q), or per-batch transcript spot-checks for out-of-arm
   reads accepted in writing for that batch.
3. ~~Arms are not one-factor treatments~~ **Closed by DATA-2164**: arms are
   now built additively from one substrate with per-arm manifests and
   integrity gates (dead-link + canary-leakage checks), so bare/knowledge/full
   differ only by declared treatment layers, not by incidental repo access or
   description leakage. The intent-card driver (surfacing which skill section
   an answer actually used) is deferred to its own follow-up ticket under
   DATA-2142; the interim is the noninteractive contract plus the
   pre-first-batch skill-engagement probe (checklist item 2 above).
4. Evidence artifacts and execution metadata — partially closed 2026-07-24:
   run_matrix now records per-run cost/tokens/turns into state.json (surfaced
   as scores.csv columns and a per-arm cost section in report.md), and judge
   verdicts persist to `<run_id>.judge.json` with model, key id, and answer
   hash (reused on regrade; `--rejudge` forces). Still open under ClickUp
   86ajmykh4: per-check evidence records (source checks are still transcript
   greps) and the immutable BatchConfig bundle.
5. Protocol items (randomized/paired schedule, rule 2 effect size, rule 3
   non-inferiority margin, holdout enforcement, frozen snapshot) are
   unimplemented — verdicts are pre-registered directionally but not yet
   defensible as an experiment. Gate: ClickUp 86ajmykh4.

## Arm construction (amended 2026-07-22, DATA-2164)

**Pre-registration amendment (Tristan sign-off 2026-07-21):** the full arm is
no longer "the repo as deployed" (a git-archive export of main). All arms are
built additively from one substrate: bare = substrate; knowledge = substrate
+ the two product-knowledge skills + their analysis libs; full = knowledge +
the analytics-process skill + its two claimed reviewer agents. The claim under
test is therefore "the skills as a treatment," not "the repo as deployed."
Rationale: subtractive arms were not one-factor contrasts (dead cross-skill
links in the knowledge arm; repo content only in full/knowledge; curated dbt
descriptions leaking into the floor), so wins were not attributable to the
intended treatment (PR #636 human review).

`prep_arms.py` exports treatment layers from `--ref` (default `main`) via
per-path `git archive`, writes a per-arm `manifest.json` (file, sha256,
contributing layer), and fails unless integrity passes: every relative md
link resolves inside the arm, and no canary phrase (canaries.yaml) from an
excluded layer appears anywhere in the arm or the floor. Batch resume is
gated on the manifest hashes (state.json `arms_sha256`).

Until this branch merges, pass `--ref data-2164-additive-arms` (or whichever
branch carries the current skill/lib content) explicitly, since `main` may
lag the layers under test:

    uv run python diagnostics/quality_bench/prep_arms.py --ref data-2164-additive-arms

After the branch merges, the default (`main`) is correct and the flag can be
dropped.

## Invocation

Three CLIs in this directory (`prep_arms.py`, `run_matrix.py`, `grade.py`)
bootstrap `sys.path` via a `try/except ImportError` block at the module top
to import from the quality_bench package. `floor_gen.py` is different: it
imports nothing from quality_bench; instead, it bootstraps `sys.path` inside
its `__main__` block to import `databricks_conn` from `analytics/lib`. In all
cases, `uv run python diagnostics/quality_bench/<tool>.py ...` works directly
from `analytics/` — no `PYTHONPATH` fiddling or `-m` invocation needed.

## Run a batch

    cd analytics
    uv run python diagnostics/quality_bench/floor_gen.py         # refresh floor.md
    uv run python diagnostics/quality_bench/prep_arms.py          # build 3 arm dirs
    uv run python diagnostics/quality_bench/run_matrix.py --batch 2026-07-20 --reps 3
    uv run python diagnostics/quality_bench/grade.py --batch 2026-07-20 --judge

Outputs: `results/<batch>/scores.csv`, `results/<batch>/report.md` (gitignored).
Resume: rerunning run_matrix with the same --batch skips completed runs.

Before trusting a batch verdict, run the isolation spot-check (no-decision
gate 2 interim) and record acceptance in writing on the ticket:

    uv run python diagnostics/quality_bench/spot_check.py --batch 2026-07-20

It scans every run transcript for filesystem access outside the run's own
dir/HOME/tmp/system prefixes and exits 1 with per-run flag lines if any need
review. Flags are review items, not verdicts.

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
    uv run python diagnostics/quality_bench/prep_arms.py --ref data-2164-additive-arms
    uv run python diagnostics/quality_bench/run_matrix.py --batch smoke --reps 1 --arms bare full --model sonnet
    uv run python diagnostics/quality_bench/grade.py --batch smoke

**Rep-threshold note:** verdict rule 1 (`rule1_trustworthy`) requires passing
≥2/3 reps, and cell consistency (`grading.cell_consistency`) needs ≥2 parsed
reps to say anything about spread. With `--reps 1` the verdict fields render
as False / inf / None depending on the field — none of them are meaningful
verdicts. That is expected, not a bug. The smoke's actual success criterion is:

- both runs (`bare`, `full`) have `ok=true` in `state.json`,
- `results/smoke/scores.csv` has 2 rows, both `numbers_pass=True`,
- `results/smoke/report.md` renders with score/state rows present (the verdict
  and consistency fields are noise at reps=1; ignore them).

Debug notes: if a run fails, read the copied transcript in `results/smoke/`;
if the transcript is missing, check the munge in `transcript_path` against the
actual dir name under `~/.claude/projects/`.
