"""DATA-2225 Area 0a: frozen evaluation harness.

One yardstick for every v2 experiment: frozen fold assignments, frozen forward-split
membership, a versioned working set, and one append-only results ledger. Fold logic
mirrors mvp_model.py exactly (StratifiedGroupKFold k=5, shuffle, seed 7, grouped by
user_id) so the v1 numbers reproduce on the frozen folds.

Usage (from analytics/):
  uv run python projects/legit_candidate_predictor/harness.py --freeze
  uv run python projects/legit_candidate_predictor/harness.py --verify

--freeze  writes datasets/folds_<wsver>.parquet for the current working set.
--verify  recomputes the v1 baselines from cached OOF predictions (and one forward
          refit), asserts they match results.json / SESSION_NOTES, then seeds the
          ledger. Refuses to seed if any check fails.

Every experiment appends via append_ledger(); entries carry the working-set version
so numbers from different data vintages are never compared silently.
"""

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold

HERE = Path(__file__).resolve().parent
DATASETS = HERE / "datasets"
WORKING_SET = DATASETS / "working_set.parquet"
LEDGER = HERE / "results_ledger.jsonl"

# v1 reference numbers (results.json + SESSION_NOTES rev 8). verify() must reproduce
# the recomputable ones before the ledger is seeded.
V1 = {
    "hgb_roc_auc": 0.875,
    "hgb_pr_auc": 0.8878,
    "lr_roc_auc": 0.857,
    "lr_pr_auc": 0.8725,
    "decidable_hgb_roc_auc": 0.8258,
    "decidable_lr_roc_auc": 0.8005,
    "forward_hgb_roc_auc": 0.896,  # train 2025 -> test completed 2026
    "no_proxy_floor_roc_auc": 0.842,  # recorded from v1 ablation, not recomputed here
    "pass_a_hgb_roc_auc": 0.887,  # recorded, diagnostic only
    "n": 20480,
    "decidable_n": 18021,
}


def ws_version(path: Path = WORKING_SET) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def folds_path(ver: str) -> Path:
    return DATASETS / f"folds_{ver}.parquet"


def decidable_mask(df: pd.DataFrame) -> pd.Series:
    """Rows the model owns; the null-BR-position bucket is rule-decided, not modeled."""
    m = df["br_normalized_position_type"].notna()
    if m.dtype != bool:
        m = m.astype(bool)
    return m & df["br_normalized_position_type"].astype(str).ne("missing")


def forward_role(df: pd.DataFrame) -> pd.Series:
    year = pd.to_datetime(df["general_election_date"]).dt.year
    return pd.Series(
        np.select([year.eq(2025), year.ge(2026)], ["train_2025", "test_2026plus"], "none"),
        index=df.index,
    )


def freeze_folds(df: pd.DataFrame, ver: str) -> pd.DataFrame:
    y = df["corroborated"].astype(int)
    cv = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=7)
    fold = np.full(len(df), -1, dtype=int)
    for i, (_tr, va) in enumerate(cv.split(df, y, df["user_id"])):
        fold[va] = i
    out = pd.DataFrame(
        {
            "campaign_id": df["campaign_id"].values,
            "user_id": df["user_id"].values,
            "fold": fold,
            "forward_role": forward_role(df).values,
            "decidable": decidable_mask(df).values,
        }
    )
    assert (out["fold"] >= 0).all()
    assert out["campaign_id"].is_unique, "campaign_id is not a unique key for folds"
    out.to_parquet(folds_path(ver), index=False)
    return out


def load_folds(ver: str | None = None) -> pd.DataFrame:
    ver = ver or ws_version()
    p = folds_path(ver)
    if not p.exists():
        raise FileNotFoundError(f"{p.name} not frozen for working set {ver}; run harness.py --freeze")
    return pd.read_parquet(p)


def evaluate(y, p, decidable=None) -> dict:
    y = np.asarray(y).astype(int)
    p = np.asarray(p, dtype=float)
    out = {
        "n": int(len(y)),
        "positive_rate": round(float(y.mean()), 4),
        "roc_auc": round(float(roc_auc_score(y, p)), 4),
        "pr_auc": round(float(average_precision_score(y, p)), 4),
    }
    if decidable is not None:
        d = np.asarray(decidable, dtype=bool)
        out["decidable_n"] = int(d.sum())
        out["decidable_positive_rate"] = round(float(y[d].mean()), 4)
        out["decidable_roc_auc"] = round(float(roc_auc_score(y[d], p[d])), 4)
        out["decidable_pr_auc"] = round(float(average_precision_score(y[d], p[d])), 4)
    return out


MANIFESTS = HERE / "manifests"
CODE_FILES = ("mvp_model.py", "harness.py")


def _git(*args: str) -> str:
    """Best-effort git metadata. Never raises: a missing repo must not block an experiment."""
    import subprocess

    try:
        out = subprocess.run(["git", *args], cwd=HERE, capture_output=True, text=True, timeout=10)
        return out.stdout.strip() if out.returncode == 0 else ""
    except Exception:
        return ""


def model_manifest(
    name: str, *, features: dict[str, list[str]], excluded: dict[str, str], hyperparams: dict, notes: str = ""
) -> dict:
    """Write a provenance record for one model and return it, including a stable `model_id`.

    Answers, from the artifact alone: which model produced these predictions, what went into it,
    what was deliberately kept out and why, which working set it saw, and which code built it.
    Attach the `model_id` to every scored output and to every analysis drawn from those scores.

    `excluded` is a column -> reason map and is the important half. A model is defined as much by
    what it was denied as by what it was given: reserved-for-analysis columns, leakage channels,
    and coverage proxies all need to be legible later, when the reasoning has been forgotten.
    """
    payload = {
        "name": name,
        "ws_version": ws_version(),
        "features": {k: sorted(v) for k, v in features.items()},
        "excluded": dict(sorted(excluded.items())),
        "hyperparams": dict(sorted(hyperparams.items())),
        "code_sha": {
            f: hashlib.sha256((HERE / f).read_bytes()).hexdigest()[:12]
            for f in CODE_FILES
            if (HERE / f).exists()
        },
    }
    # model_id covers everything that changes predictions, so two runs share an id only if they
    # would produce the same scores. Git state and the date are recorded but deliberately excluded
    # from the hash, so a comment-only commit does not invent a new model.
    payload["model_id"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]
    payload["date"] = date.today().isoformat()
    payload["git_commit"] = _git("rev-parse", "HEAD")
    payload["git_dirty"] = bool(_git("status", "--porcelain"))
    payload["notes"] = notes

    MANIFESTS.mkdir(exist_ok=True)
    (MANIFESTS / f"{payload['model_id']}.json").write_text(json.dumps(payload, indent=2) + "\n")
    return payload


# --- MLflow dual-write (DATA-2248 Phase 1, 2026-08-11) ---
# The JSONL stays the human-readable mirror and offline fallback; MLflow is the
# queryable copy. Auth: the OAuth profile, NOT the PAT (which lacks the mlflow
# scope). Requires mlflow-skinny (add --with mlflow-skinny to the uv run line);
# any failure degrades to a warning so tracking never blocks an experiment.
MLFLOW_TRACKING_URI = "databricks://tristana_goodparty"
MLFLOW_EXPERIMENT = "/Shared/legit_candidate_predictor"


def _mlflow_log(entry: dict) -> str | None:
    """Mirror one ledger entry to MLflow; returns the run_id or None on failure."""
    try:
        import mlflow

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT)
        with mlflow.start_run(run_name=entry["experiment"]) as run:
            mlflow.log_params(
                {
                    k: entry[k]
                    for k in ("split", "model", "ws_version", "model_id", "git_commit")
                    if entry.get(k)
                }
            )
            mlflow.log_metrics(
                {k: float(v) for k, v in entry["metrics"].items() if isinstance(v, int | float)}
            )
            mlflow.set_tags({"area": entry["area"], "notes": entry["notes"], "ledger_date": entry["date"]})
            if entry.get("model_id"):
                manifest = MANIFESTS / f"{entry['model_id']}.json"
                if manifest.exists():
                    mlflow.log_artifact(str(manifest))
            return run.info.run_id
    except Exception as e:  # tracking must never block an experiment
        print(f"WARNING: mlflow mirror failed ({type(e).__name__}: {e}); JSONL row kept")
        return None


def append_ledger(
    experiment: str,
    split: str,
    model: str,
    metrics: dict,
    notes: str = "",
    area: str = "",
    ver: str | None = None,
    model_id: str | None = None,
) -> dict:
    entry = {
        "date": date.today().isoformat(),
        "experiment": experiment,
        "area": area,
        "split": split,  # oof_cv5 | forward_2025to2026 | other (say in notes)
        "model": model,
        "ws_version": ver or ws_version(),
        "model_id": model_id,  # -> manifests/<model_id>.json for the full feature declaration
        "git_commit": _git("rev-parse", "HEAD"),
        "metrics": metrics,
        "notes": notes,
    }
    entry["mlflow_run_id"] = _mlflow_log(entry)
    with LEDGER.open("a") as f:
        f.write(json.dumps(entry) + "\n")
    return entry


def _check(name: str, got: float, want: float, tol: float) -> bool:
    ok = abs(got - want) <= tol
    print(f"  {'OK ' if ok else 'FAIL'} {name}: got {got:.4f}, v1 {want:.4f} (tol {tol})")
    return ok


def verify_and_seed() -> None:
    ver = ws_version()
    df = pd.read_parquet(WORKING_SET)
    oof = pd.read_parquet(DATASETS / "oof_predictions.parquet")
    print(f"working set {ver}: n={len(df):,}")

    merged = oof.merge(
        df[["campaign_id", "br_normalized_position_type", "general_election_date"]],
        on="campaign_id",
        how="left",
        validate="one_to_one",
    )
    assert len(merged) == len(oof)
    y = merged["corroborated"].astype(int)
    dmask = decidable_mask(merged)

    checks = [_check("n", len(merged), V1["n"], 0)]
    checks.append(_check("decidable_n", int(dmask.sum()), V1["decidable_n"], 0))

    results = {}
    for m in ("hgb", "lr"):
        results[m] = evaluate(y, merged[f"oof_{m}"], dmask)
        checks.append(_check(f"{m}_roc_auc", results[m]["roc_auc"], V1[f"{m}_roc_auc"], 5e-4))
        checks.append(_check(f"{m}_pr_auc", results[m]["pr_auc"], V1[f"{m}_pr_auc"], 5e-4))
        # v1's decidable numbers came from the independent validator's own refit, not
        # these cached OOF preds (pooled matches to 4dp, decidable differs by ~8e-4).
        # The harness recomputation is canonical going forward; tolerance covers the
        # refit noise and the discrepancy is recorded in the ledger notes.
        checks.append(
            _check(
                f"decidable_{m}_roc_auc", results[m]["decidable_roc_auc"], V1[f"decidable_{m}_roc_auc"], 2e-3
            )
        )

    # Forward split needs one refit (no cached predictions exist for it).
    sys.path.insert(0, str(HERE))
    from mvp_model import engineer, make_hgb_frame

    eng = engineer(df)
    role = forward_role(eng)
    x = make_hgb_frame(eng)
    ytr = eng.loc[role.eq("train_2025"), "corroborated"].astype(int)
    yte = eng.loc[role.eq("test_2026plus"), "corroborated"].astype(int)
    hgb = HistGradientBoostingClassifier(
        categorical_features="from_dtype",
        random_state=7,
        max_iter=400,
        early_stopping=True,
        validation_fraction=0.15,
    ).fit(x[role.eq("train_2025")], ytr)
    p_fwd = hgb.predict_proba(x[role.eq("test_2026plus")])[:, 1]
    fwd = evaluate(yte, p_fwd)
    checks.append(_check("forward_hgb_roc_auc", fwd["roc_auc"], V1["forward_hgb_roc_auc"], 5e-3))

    if not all(checks):
        raise SystemExit("verification FAILED; ledger not seeded")

    if LEDGER.exists():
        print(f"\n{LEDGER.name} already exists; not re-seeding (delete it to re-seed)")
        return

    base = dict(area="v1-baseline", ver=ver)
    append_ledger(
        "v1 pass B",
        "oof_cv5",
        "hgb",
        results["hgb"],
        notes="MVP rev 8 headline; recomputed from cached OOF preds. v1 recorded "
        "decidable 0.8258 from the validator's refit; 0.8250 here is canonical",
        **base,
    )
    append_ledger(
        "v1 pass B",
        "oof_cv5",
        "lr",
        results["lr"],
        notes="standalone candidate model; recomputed from cached OOF preds",
        **base,
    )
    append_ledger(
        "v1 pass B forward",
        "forward_2025to2026",
        "hgb",
        fwd,
        notes="refit at verify time, same hyperparams/seed",
        **base,
    )
    append_ledger(
        "v1 no-coverage-proxy floor",
        "oof_cv5",
        "hgb",
        {"roc_auc": V1["no_proxy_floor_roc_auc"]},
        notes="RECORDED from v1 ablation.py, not recomputed",
        **base,
    )
    append_ledger(
        "v1 pass A (diagnostic)",
        "oof_cv5",
        "hgb",
        {"roc_auc": V1["pass_a_hgb_roc_auc"]},
        notes="RECORDED from v1; activity features, never the analysis filter",
        **base,
    )
    print(f"\nledger seeded -> {LEDGER.name} (5 baseline rows, ws {ver})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--freeze", action="store_true")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    if not (args.freeze or args.verify):
        raise SystemExit("pass --freeze and/or --verify")
    if args.freeze:
        ver = ws_version()
        folds = freeze_folds(pd.read_parquet(WORKING_SET), ver)
        print(f"froze folds -> {folds_path(ver).name}")
        print(folds.groupby("fold").size().to_string())
        print(folds["forward_role"].value_counts().to_string())
    if args.verify:
        verify_and_seed()
