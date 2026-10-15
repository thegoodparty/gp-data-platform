"""Generic Area 2 experiment: add feature families to the pass-B baseline and score them.

One runner for every family, so a new source needs a build script and an intake review but not a
bespoke experiment. Reports pooled, decidable, and forward metrics against the canonical ledger
baseline, the families' standalone discrimination, permutation importances for the new columns,
and optional ablations.

Families are named through `mvp_model.FAMILY_SPECS`, which owns each family's accepted column
set. Prefer `--family` over `--features/--key/--exclude`: the ad-hoc form is what let the
post-DATA-2312 re-scores silently drop their exclusion lists (2026-08-14).

Run:
  uv run --with scikit-learn --with pyarrow --with mlflow-skinny python \
      projects/legit_candidate_predictor/exp_family.py \
      --family incumbency --name "person-level incumbency"
  (mlflow-skinny powers the --ledger MLflow mirror; without it the row still lands
  in the JSONL with a warning)

  # the v2 serving composition, with each family's marginal contribution measured
  ... --family br_position gp_user_details incumbency --loo --manifest

  # an analysis-tier variant: mask a claim's columns, backfill from the L2 block
  ... --family br_position gp_user_details incumbency --claim incumbency --substitute l2_easy

  # drop a sub-block to see how much of the lift it owns
  ... --ablate inc_holds_same_position inc_serving_at_signup
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import (  # noqa: E402
    CAT_COLS,
    CLAIM_MASKS,
    FAMILY_SPECS,
    NUM_COLS,
    FamilySpec,
    engineer,
    make_hgb_frame,
    recency_weights,
)

# Canonical since the DATA-2312 working-set rebuild (ws b04cdd5ac4b1, ledger
# "ws-rebuild pass 3 CANONICAL" rows, 2026-08-13). Recency spec unchanged (hl=240d).
# Supersedes 0.8683/0.8848/0.8155/0.8914 on ws 6fd10aa234d3; deltas measured against
# that older row are NOT comparable to deltas measured against this one.
# Forward fell 0.8914 -> 0.8669 purely because the forward test grew from 1,181 to
# 1,444 rows: on the 1,180 shared rows this model scores 0.8942, slightly BETTER than
# the row it replaces. The new rows are a long-lead single-state cohort that
# corroborates at 25%, which the model ranks poorly (0.6448).
BASELINE = {"roc_auc": 0.8732, "pr_auc": 0.8860, "decidable_roc_auc": 0.8240, "forward_roc_auc": 0.8669}
HGB = dict(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=400,
    early_stopping=True,
    validation_fraction=0.15,
)


def load(specs: dict[str, FamilySpec]) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """Attach every named family to the working set. Returns the frame and each family's columns.

    Excluded columns are never merged in, so a downstream typo cannot resurrect one.
    """
    df = pd.read_parquet(harness.WORKING_SET)
    folds = harness.load_folds()
    df = df.merge(
        folds[["campaign_id", "fold", "forward_role", "decidable"]],
        on="campaign_id",
        how="left",
        validate="one_to_one",
    )
    assert df["fold"].notna().all()

    cols_by_family: dict[str, list[str]] = {}
    for name, spec in specs.items():
        fam = pd.read_parquet(HERE / "datasets" / spec.file)
        # An exclusion that names a column the parquet does not have is a stale list, not a
        # no-op: it means the family was rebuilt and the decision was never revisited.
        stale = sorted(set(spec.exclude) - set(fam.columns))
        assert not stale, f"{name}: exclusion list names columns not in {spec.file}: {stale}"
        cols = [c for c in fam.columns if c != spec.key and c not in spec.exclude]
        clash = sorted(set(cols) & set(df.columns))
        assert not clash, f"{name}: column names already present in the frame: {clash}"
        before = len(df)
        df = df.merge(fam[[spec.key, *cols]], on=spec.key, how="left", validate="many_to_one")
        assert len(df) == before, f"{name} merge changed the row count"
        cols_by_family[name] = cols
    return df, cols_by_family


def frame(df: pd.DataFrame, eng: pd.DataFrame, cols: list[str], mask: tuple[str, ...] = ()) -> pd.DataFrame:
    """Assemble the model matrix, then drop the masked columns.

    Masking happens on the ASSEMBLED frame so a claim mask can reach baseline columns
    (CAT_COLS / NUM_COLS) as well as family ones. --exclude never could.
    """
    x = make_hgb_frame(eng)
    for c in cols:
        s = df[c]
        if s.dtype == object or isinstance(s.dtype, pd.CategoricalDtype):
            x[c] = pd.Categorical(s.astype("object").where(s.notna(), "missing"))
        else:
            x[c] = pd.to_numeric(s, errors="coerce").to_numpy()
    if mask:
        absent = [c for c in mask if c not in x.columns]
        assert not absent, f"mask names columns not in the model frame: {absent}"
        x = x.drop(columns=list(mask))
    return x


def oof_scores(x: pd.DataFrame, y: pd.Series, fold: pd.Series, w: np.ndarray, cols: list[str] | None = None):
    """Out-of-fold predictions on the frozen folds. `cols` restricts to a sub-matrix."""
    use = x if cols is None else x[cols]
    oof = np.zeros(len(x))
    for k in sorted(fold.unique()):
        tr, va = fold.ne(k), fold.eq(k)
        m = HistGradientBoostingClassifier(**HGB).fit(use[tr], y[tr], sample_weight=w[tr.to_numpy()])
        oof[va.to_numpy()] = m.predict_proba(use[va])[:, 1]
    return oof


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--family", nargs="*", default=[], help=f"registry family names to compose: {', '.join(FAMILY_SPECS)}"
    )
    ap.add_argument("--features", help="ad-hoc parquet under datasets/ (prefer --family)")
    ap.add_argument("--key", help="join key for --features")
    ap.add_argument("--name", required=True, help="ledger experiment name")
    ap.add_argument("--ablate", nargs="*", default=[], help="columns to drop in a second run")
    ap.add_argument(
        "--exclude",
        nargs="*",
        default=[],
        help="ad-hoc exclusions for --features; registry families carry their own",
    )
    ap.add_argument(
        "--mask",
        nargs="*",
        default=[],
        help="columns to drop from the assembled frame, baseline ones included",
    )
    ap.add_argument(
        "--claim",
        choices=sorted(CLAIM_MASKS),
        help="analysis tier: mask every column a claim about this concept would restate",
    )
    ap.add_argument(
        "--substitute",
        choices=sorted(FAMILY_SPECS),
        help="family added as a substitution basis to backfill a masked claim",
    )
    ap.add_argument(
        "--loo",
        action="store_true",
        help="leave-one-family-out ablations (marginal contribution inside the mix)",
    )
    ap.add_argument("--manifest", action="store_true", help="write a manifests/<model_id>.json")
    ap.add_argument("--ledger", action="store_true", help="append a ledger row")
    ap.add_argument("--notes", default="")
    args = ap.parse_args()

    specs: dict[str, FamilySpec] = {n: FAMILY_SPECS[n] for n in args.family}
    if args.features:
        assert args.key, "--features needs --key"
        specs[Path(args.features).stem] = FamilySpec(
            file=args.features, key=args.key, exclude={c: "ad-hoc --exclude" for c in args.exclude}
        )
    assert specs, "name at least one --family or --features"
    if args.substitute:
        assert args.substitute not in specs, "--substitute family is already in the composition"
        specs[args.substitute] = FAMILY_SPECS[args.substitute]

    mask = tuple(args.mask) + tuple(CLAIM_MASKS[args.claim] if args.claim else ())
    tier = "analysis" if args.claim else "serving"

    df, cols_by_family = load(specs)
    cols = [c for fam_cols in cols_by_family.values() for c in fam_cols]
    eng = engineer(df)
    y = eng["corroborated"].astype(int)
    fold = eng["fold"]
    w = recency_weights(eng)  # canonical spec since the hl=240d re-base (2026-08-11)
    x = frame(df, eng, cols, mask)
    cols = [c for c in cols if c not in mask]  # solo scoring must respect the mask too

    print(f"=== {args.name} [{tier} tier] ===")
    for name, fam_cols in cols_by_family.items():
        kept = [c for c in fam_cols if c not in mask]
        reach = df[fam_cols[0]].notna().mean()
        dropped = len(fam_cols) - len(kept)
        excluded = len(FAMILY_SPECS[name].exclude) if name in FAMILY_SPECS else 0
        print(
            f"  {name}: {len(kept)} columns, {reach:.1%} reach"
            f"{f', {excluded} excluded by registry' if excluded else ''}"
            f"{f', {dropped} masked by claim' if dropped else ''}"
        )
    if mask:
        print(f"  claim mask ({args.claim}): {len(mask)} columns removed from the frame")
    print(f"  model matrix: {x.shape[1]} columns\n")

    oof = oof_scores(x, y, fold, w)
    metrics = harness.evaluate(y, oof, eng["decidable"])
    print(f"\n=== base + {args.name} (frozen OOF) ===\n{metrics}")
    for k in ("roc_auc", "pr_auc", "decidable_roc_auc"):
        print(f"{k}: {metrics[k]:.4f}  baseline {BASELINE[k]:.4f}  " f"delta {metrics[k] - BASELINE[k]:+.4f}")

    solo = oof_scores(x, y, fold, w, cols=cols)
    print(f"\nfamily ALONE (OOF): roc_auc {roc_auc_score(y, solo):.4f}")

    if args.ablate:
        keep = [c for c in cols if c not in args.ablate]
        abl = oof_scores(frame(df, eng, keep, mask), y, fold, w)
        am = harness.evaluate(y, abl, eng["decidable"])
        print(
            f"\nminus {args.ablate}: pooled {am['roc_auc']:.4f}  " f"decidable {am['decidable_roc_auc']:.4f}"
        )

    loo: dict[str, dict] = {}
    if args.loo and len(cols_by_family) > 1:
        # What each family is worth INSIDE the mix, which is not what it was worth alone on
        # top of the baseline. Overlapping families each look smaller here, and that gap is
        # the shared-signal measurement.
        print("\n=== leave-one-family-out (marginal contribution inside the composition) ===")
        for name, fam_cols in cols_by_family.items():
            keep = [c for c in cols if c not in fam_cols]
            lm = harness.evaluate(y, oof_scores(frame(df, eng, keep, mask), y, fold, w), eng["decidable"])
            loo[name] = {"pooled": lm["roc_auc"], "decidable": lm["decidable_roc_auc"]}
            print(
                f"  minus {name:<18} pooled {lm['roc_auc']:.4f} "
                f"({lm['roc_auc'] - metrics['roc_auc']:+.4f})  "
                f"decidable {lm['decidable_roc_auc']:.4f} "
                f"({lm['decidable_roc_auc'] - metrics['decidable_roc_auc']:+.4f})"
            )

    role = eng["forward_role"]
    tr, te = role.eq("train_2025"), role.eq("test_2026plus")
    fwd_model = HistGradientBoostingClassifier(**HGB).fit(x[tr], y[tr], sample_weight=w[tr.to_numpy()])
    fwd = harness.evaluate(y[te], fwd_model.predict_proba(x[te])[:, 1])
    print(f"\n=== forward 2025 -> 2026 ===\n{fwd}")
    print(
        f"roc_auc {fwd['roc_auc']:.4f}  baseline {BASELINE['forward_roc_auc']:.4f}  "
        f"delta {fwd['roc_auc'] - BASELINE['forward_roc_auc']:+.4f}"
    )

    r = permutation_importance(fwd_model, x[te], y[te], n_repeats=5, random_state=7, scoring="roc_auc")
    imp = pd.Series(r.importances_mean, index=x.columns)
    print("\nnew-column importances (permutation, forward test):")
    print(imp[cols].sort_values(ascending=False).head(12).to_string())

    stem = Path(args.features).stem if args.features else "_".join(cols_by_family)
    if args.claim:
        stem = f"{stem}_no_{args.claim}"
    out = eng[["user_id", "campaign_id", "corroborated", "fold"]].copy()
    out["decidable"] = eng["decidable"].to_numpy(dtype=bool)
    out["oof_hgb"] = oof
    out["oof_family_only"] = solo
    out.to_parquet(HERE / "datasets" / f"oof_{stem}.parquet", index=False)
    print(f"\nsaved datasets/oof_{stem}.parquet")

    # What the model was DENIED is half its definition: registry exclusions (never admissible)
    # and claim masks (admissible, held back so the analysis tier can speak about them).
    excluded = {c: f"{name}: {why}" for name, spec in specs.items() for c, why in spec.exclude.items()}
    excluded |= {
        c: f"analysis tier: reserved so claims about {args.claim} are not "
        f"restatements of the scoring rule"
        for c in mask
    }

    model_id = None
    if args.manifest:
        man = harness.model_manifest(
            args.name,
            features={
                "cat": [c for c in CAT_COLS if c in x.columns],
                "num": [c for c in NUM_COLS if c in x.columns],
                "family": cols,
            },
            excluded=excluded,
            hyperparams=HGB
            | {"recency_half_life_days": 240, "tier": tier, "families": sorted(cols_by_family)},
            notes=args.notes,
        )
        model_id = man["model_id"]
        print(f"manifest -> manifests/{model_id}.json")

    if args.ledger:
        metrics["forward_roc_auc"] = fwd["roc_auc"]
        metrics["family_alone_roc_auc"] = round(float(roc_auc_score(y, solo)), 4)
        for name, m in loo.items():
            metrics[f"loo_minus_{name}_roc_auc"] = m["pooled"]
            metrics[f"loo_minus_{name}_decidable"] = m["decidable"]
        # The mask and the exclusions travel WITH the row. The post-DATA-2312 re-scores were
        # unreadable precisely because they did not.
        provenance = (
            f" | tier={tier} families={sorted(cols_by_family)}"
            f" registry_excluded={sorted(c for name, s in specs.items() for c in s.exclude)}"
            f" claim_mask={args.claim or 'none'}={sorted(mask)}"
            f" substitute={args.substitute or 'none'}"
        )
        harness.append_ledger(
            args.name,
            "oof_cv5",
            "hgb",
            metrics,
            notes=args.notes + provenance,
            area="area2",
            model_id=model_id,
        )
        print("ledger row appended")
