"""Re-base the canonical baseline with recency weighting (hl=240d, signup clock).

Decision (Tristan, 2026-08-11): gentle 240-day half-life, one spec everywhere.
Supersedes the unweighted canonical rows (HGB 0.8695 / decidable 0.8172 /
forward 0.8912). This script computes the re-based rows on the frozen folds and
appends them to the ledger with a manifest; oof preds saved separately so the
v1 verification artifact (oof_predictions.parquet) is untouched.

Run (from analytics/):
  uv run --with scikit-learn --with pyarrow python \
      projects/legit_candidate_predictor/rebase_recency_baseline.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import (  # noqa: E402
    CAT_COLS,
    NUM_COLS,
    RECENCY_HALF_LIFE_DAYS,
    engineer,
    make_hgb_frame,
    make_lr,
    recency_weights,
)

HGB = dict(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=400,
    early_stopping=True,
    validation_fraction=0.15,
)

df = pd.read_parquet(harness.WORKING_SET)
folds = harness.load_folds()
df = df.merge(
    folds[["campaign_id", "fold", "forward_role", "decidable"]],
    on="campaign_id",
    how="left",
    validate="one_to_one",
)
eng = engineer(df)
y = eng["corroborated"].astype(int)
fold = eng["fold"]
dec = eng["decidable"]
x_hgb = make_hgb_frame(eng)
x_lr = eng[CAT_COLS + NUM_COLS]
w = recency_weights(eng)
ver = harness.ws_version()
print(
    f"ws {ver}  n={len(eng):,}  hl={RECENCY_HALF_LIFE_DAYS}d  "
    f"weight quartiles={np.percentile(w, [25, 50, 75]).round(3)}"
)

# --- OOF cv5, both models, weighted training ---
oof = {"hgb": np.zeros(len(eng)), "lr": np.zeros(len(eng))}
for k in sorted(fold.unique()):
    tr, va = fold.ne(k), fold.eq(k)
    wtr = w[tr.to_numpy()]
    hgb = HistGradientBoostingClassifier(**HGB).fit(x_hgb[tr], y[tr], sample_weight=wtr)
    oof["hgb"][va.to_numpy()] = hgb.predict_proba(x_hgb[va])[:, 1]
    lr = make_lr().fit(x_lr[tr], y[tr], lr__sample_weight=wtr)
    oof["lr"][va.to_numpy()] = lr.predict_proba(x_lr[va])[:, 1]
    print(f"fold {k} done")

results = {m: harness.evaluate(y, oof[m], dec) for m in ("hgb", "lr")}

# --- forward split, weighted training, HGB ---
role = eng["forward_role"]
tr, te = role.eq("train_2025"), role.eq("test_2026plus")
fwd_model = HistGradientBoostingClassifier(**HGB).fit(x_hgb[tr], y[tr], sample_weight=w[tr.to_numpy()])
p_fwd = fwd_model.predict_proba(x_hgb[te])[:, 1]
fwd = harness.evaluate(y[te], p_fwd)
fwd["mean_pred"] = round(float(p_fwd.mean()), 4)

print("\n=== re-based baseline (recency hl=240d) ===")
for m in ("hgb", "lr"):
    print(f"{m}: {results[m]}")
print(f"forward hgb: {fwd}")

# --- manifest + ledger ---
manifest = harness.model_manifest(
    "passB_hgb_recency240",
    features={"cat": CAT_COLS, "num": NUM_COLS},
    excluded={
        "viability_score": "HOLD pending PIT checks (identification-vs-signup date, "
        "overwrite rate); see DATA-2279 amendment",
        "activity_features": "pass A only; pass B is the analysis filter",
        "post_signup_candidacy_fields": "PIT allowlist (brief 3b)",
    },
    hyperparams={**HGB, "sample_weight": f"recency hl={RECENCY_HALF_LIFE_DAYS}d signup clock"},
    notes="Canonical baseline re-base: exponential recency weights folded into training "
    "(both models), no hard window cutoff. hl=240 over the sweep's 120 by OOF cost "
    "(-0.0012 vs -0.0040) with forward difference within noise; Tristan 2026-08-11.",
)
mid = manifest["model_id"]

common = dict(area="baseline-rebase", ver=ver, model_id=mid)
harness.append_ledger(
    "recency re-base (hl=240d)",
    "oof_cv5",
    "hgb",
    results["hgb"],
    notes="NEW CANONICAL BASELINE. Supersedes unweighted 0.8695/0.8844/0.8172. Weighted "
    "training costs -0.0012 pooled / -0.0017 decidable vs unweighted; adopted for "
    "forward calibration + training-mix-tracks-scoring-mix. Compare future "
    "experiments to THIS row. 08-07 sweep script was not kept; reproduction matched "
    "qualitatively, hl ranking flipped within its declared noise band, so hl=240 "
    "(gentler, near-free on OOF) chosen over 120.",
    **common,
)
harness.append_ledger(
    "recency re-base (hl=240d)",
    "oof_cv5",
    "lr",
    results["lr"],
    notes="standalone candidate model, same weighted spec",
    **common,
)
harness.append_ledger(
    "recency re-base (hl=240d)",
    "forward_2025to2026",
    "hgb",
    fwd,
    notes="NEW CANONICAL FORWARD. Supersedes unweighted 0.8912. mean_pred vs actual "
    f"{y[te].mean():.3f} — calibration carry-over from the sweep's finding.",
    **common,
)

out = eng[["user_id", "campaign_id", "corroborated", "fold"]].copy()
out["decidable"] = dec.to_numpy(dtype=bool)
out["oof_hgb"], out["oof_lr"] = oof["hgb"], oof["lr"]
out.to_parquet(HERE / "datasets" / "oof_predictions_recency240.parquet", index=False)
print(f"\nledger rows appended (model_id {mid}); saved datasets/oof_predictions_recency240.parquet")
