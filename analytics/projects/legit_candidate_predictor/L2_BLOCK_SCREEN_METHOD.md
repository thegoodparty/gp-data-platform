# L2/haystaq full-block screen: methodology and results (DATA-2239, 2026-08-13)

Reusable protocol for answering "does feature block X add anything on top of the
canonical baseline?" — written for the L2/haystaq surface but designed to be rerun
verbatim when circumstances change. This document is the scrutiny artifact: every step
names its script, its output, and the honesty rule it enforces.

## When to rerun this

- After DATA-2244 (matcha person-match) lifts resolution 51% -> ~79% (expected: no AUC
  change, per the matched-only reads below — but that expectation is now cheap to test).
- After a material covariate shift (the population composition churns continuously;
  adversarial separation 2025 vs 2026 was 0.94).
- If the label changes (PU-corrected realness label, cleanlab-excluded rerun, survival
  framing). NOTE: the standing caveat — this screen measures lift on p(corroborated).
  Web verification showed corroboration misses ~26% of real candidates non-monotonically,
  so a "real registered human" block can be informative about realness and still flat
  here. A realness-side rerun needs the Area 1 label first.
- When a NEW wide block arrives (HubSpot properties, a future data vendor): steps 1-6
  generalize; only the rule tables in step 1 are surface-specific.

## The pipeline (scripts in this directory, artifacts in `datasets/`)

| step | script | output | honesty rule |
|---|---|---|---|
| 1. mechanical triage | `screen_l2_block.py` | `l2_block_dispositions.csv` | label-free; ordered rule table (id/PII/protected/geo/free-text) |
| 2. protected-class proxy screen | `screen_l2_proxy.py` | `l2_proxy_screen.csv`, `l2_block_candidates.csv`, cached sample `_l2_proxy_sample.parquet` | label-free; run on a deterministic nationwide L2 hash sample (`pmod(hash(LALVOTERID),2000)=0`, n=109,568), never on our users |
| 3. gated pull | `build_l2_block.py` | `l2_block_features.parquet` (+ profile, sample CSV, raw cache) | PIT gates cell-wise (see below); user_id only, LALVOTERID stays in the bridge |
| 4. readable intake | `report_l2_block.py` | `intake/l2_block_features.md` | regenerates the mechanical half; preserves decisions below the marker |
| 5. full-block experiment | `exp_family.py --features l2_block_features.parquet` | `oof_l2_block_features.parquet`, ledger row | frozen folds, NO pre-selection — flat is a legitimate negative |
| 6. univariate map + clusters | `screen_l2_univariate.py` | `l2_block_univariate.csv`, `l2_block_clusters.csv` | informativeness from TRAINING folds only; clustering label-free |
| 7. exhaustion probes | `exp_l2_subsets.py [--k K] [--only A,B]` | stdout (this doc records results) | selection redone inside each fold's training side |

### Protected-class exclusion (decided 2026-08-13, both model tiers)

Gender, ethnicity, religion, language — dropped a priori (59 columns), disability added
at the gate (2 columns). Then the proxy screen: bin every candidate (deciles for
numerics, levels for categoricals), drop it if any value with n >= 200 shifts any
protected-class posterior by >= 0.30 from base. Result: 150 dropped (118 gender-driven,
33 ethnicity, 0 religion/language). Sensitivity: 272 would drop at 0.25, 494 at 0.20.
Key finding, hand-verified: the strong proxies are haystaq's own modeled opinion scores
(the vendor visibly models from demographics — `hf_gun_control=Oppose` is 97.8% male,
`hs_gamer_no` bottom decile 98.7% male), not the consumer purchase flags. Complete
per-column audit with exact posteriors: `intake/l2_block_features.md` §2 (columns can be
individually argued back in from there). Missingness-pattern shifts are reported
separately, not auto-dropped.

### PIT gates (cell-wise, replicating the accepted easy-match family)

- Year-indexed vote history and primary-ballot party: NaN when year >= signup year.
- Registration dates -> years-at-signup, NaN when registration postdates signup.
- FEC totals only when the last donation predates signup; tri-state donor flag.
- Everything else is an a-priori snapshot attribute (L2 acquisition is independent of
  candidate arrival).
- Null semantics: for a MATCHED voter, an absent vote-history mark or Y-flag is a
  genuine 0 (vendor writes presence only); unmatched users are NaN across the block.
  Getting this wrong silently deletes the negative class — it was caught by the intake's
  learnability table showing zero negatives.

## The exhaustion protocol (the "did you really squeeze it" checklist)

Run after a flat full-block result, in order of decisiveness. All on the frozen folds
(ws 6fd10aa234d3), all vs the canonical baseline 0.8683 / 0.8155 / 0.8914
(recency hl=240d, model_id 9957e9c34fcc).

- A: top-K by individual lift, selection recomputed inside each fold's training side.
- B: as A, but max one column per label-free correlation cluster (orthogonal picks).
- C: as A, restricted to columns with >= 20% working-set coverage.
- D: the block compressed to ONE stacked column (family-alone OOF prediction). K-free
  and decisive: any best-K subset is bounded by the whole block's information, so if D
  is flat, no subset or rotation can win. Standard OOF stacking; its slight optimism is
  conservative when the expected result is flat.
- E: top-25 PCA components of the standardized hs_ block, fit per training fold
  (catches signal smeared across correlated columns).
- Matched-only read on every variant (unmatched rows are NaN by construction and drag
  pooled deltas toward zero).
- K-sensitivity on A and B (10 / 25 / 50) so the shortlist size is not a free parameter.

## Results (2026-08-13 run)

Working set 20,480 rows; block reaches 56.0% of it (21,829 bridge users; 51.1% of the
42,743-user population spine). Family alone: OOF 0.609.

| variant | pooled | decidable | forward | matched-only |
|---|---|---|---|---|
| full block, 791 cols (headline) | 0.8637 (−0.0046) | 0.8091 (−0.0064) | 0.8896 (−0.0018) | 0.8577 (−0.0062) |
| A: top 25 by lift | −0.0009 | −0.0013 | +0.0021 | −0.0015 |
| B: top 25 orthogonal | −0.0008 | −0.0011 | +0.0021 | −0.0011 |
| C: top 25, coverage >= 20% | −0.0018 | −0.0026 | +0.0005 | −0.0024 |
| D: stacked block score | **+0.0003** | **+0.0004** | +0.0059 | +0.0005 |
| E: PCA 25 of hs_ | −0.0034 | −0.0048 | +0.0031 | −0.0049 |

K-sensitivity (pooled delta, plain / orthogonal): K=10 −0.0013 / −0.0007 ·
K=25 −0.0009 / −0.0008 · K=50 −0.0021 / −0.0019. Flat at every K, slightly worse at 50
as the dilution tax returns — the shortlist size was not a hidden free parameter.

Proxy-cost diagnostic (Tristan's question — what did the fairness screen give up?):
re-including the 150 proxy-dropped columns (protected classes themselves still excluded;
`diag_l2_proxy_cost.py`, run `l2_block_plus_proxies_diag`, 941 columns) scores
**−0.0062 pooled / −0.0088 decidable / +0.0016 forward, family alone 0.6068** — worse
than the screened block on every yardstick, and no standalone lift. **The protected-class
screen cost zero AUC.** The diagnostic feature parquet and its raw cache are deleted
after measurement (they carry proxy columns and must never reach a serving composition);
the script regenerates them if the number ever needs re-deriving.

Segment-conditional read (the last protocol box, from saved OOFs): the block is
flat-to-negative in EVERY segment including where the baseline is blind — BR blind spot
(zero roster, n=6,995, baseline AUC 0.9204 there): −0.0031; blind spot AND L2-matched:
−0.0035; BR-covered: −0.0057; rule-routed position-type-missing bucket: −0.0188. No
hidden pocket where L2 pays on this label.

Baseline matched-only OOF AUC 0.8639 (n=11,467, base rate 0.626); no variant exceeds it
by more than +0.0005. D's forward +0.0059 sits at the forward slice's known ±0.005 noise
band (n=1,181) while every other read of D is flat; pooled OOF is the yardstick (2026-08-11
re-base decision). Selection stability: 13-14 of 25 picks identical across all 5 folds.

**Verdict: redundant conditional on the baseline, closed at full protocol depth.** The
dilution story is quantified: the full block's −0.0046 shrinks to −0.0009 at honest
top-25, i.e. ~80% of the full-width negative was dead-weight tax — and the recovered
subsets still never clear zero. D (+0.0003 with zero dilution possible) is the proof
that the block's standalone 0.609 is information the baseline already carries.

Third consecutive confirmation of the same verdict (l2_easy_match −0.002,
l2_lean_variant wash, full block + probes flat). Implication recorded on DATA-2244:
raising match resolution will not flip the AUC case; the ticket's value is record
correctness and analysis-tier claims.

**What survives as assets:** the bridge (`l2_match_bridge.parquet`, DATA-2244 prototype);
the block parquet as the substitution basis for analysis-model variants (drop a claim
variable, swap in its L2 cluster representative — each swap needs its own measured run);
the cluster map (381 clusters) as the substitution index; the proxy-screen method +
threshold as the standing protected-class guard for every future family.

## Scrutiny checklist (for a reviewer)

1. No label touched before step 5 — steps 1-2 run on L2's own data, step 3 is a join.
2. Selection statistics never computed on full data — grep `strengths(` call sites in
   `exp_l2_subsets.py`; the mask is always the fold's training side.
3. PIT gates: spot-check `l2b_General_<year>` is NaN for signups in or before that year;
   `l2b_fec_*` NaN when the last donation postdates signup.
4. The proxy screen's biggest claims reproduce by hand from `_l2_proxy_sample.parquet`
   (three were re-derived independently during the run: gamer, gun control, veteran).
5. Folds are the frozen `datasets/folds_6fd10aa234d3.parquet`; baseline constants match
   `exp_family.BASELINE`.
6. The 25/10/50 shortlist sizes and the 0.30/0.25/0.20 thresholds are all reported with
   sensitivity, not as single free choices.
