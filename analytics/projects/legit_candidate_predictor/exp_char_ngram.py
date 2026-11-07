"""DATA-2225 model-family experiment: char n-gram text features on the HGB baseline.

Adds fold-fitted TF-IDF char_wb(2,4) + TruncatedSVD embeddings of (a) the full name
string and (b) email_domain to the sentinel-aware pass-B feature set. All text
transformers are fit INSIDE each frozen fold on the train side only (leakage
discipline is the point). Two variants: base+name (16 SVD dims) and
base+name+domain (16+8 dims), vs the sentinel-aware baseline hgb 0.8695 / 0.8172
decidable. Does NOT touch results_ledger.jsonl or any existing file.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.feature_extraction.text import TfidfVectorizer

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import engineer, make_hgb_frame  # noqa: E402

BASELINE = {"roc_auc": 0.8695, "decidable_roc_auc": 0.8172, "forward_roc_auc": 0.8912}
RS = 7


def make_hgb():
    return HistGradientBoostingClassifier(
        categorical_features="from_dtype",
        random_state=RS,
        max_iter=400,
        early_stopping=True,
        validation_fraction=0.15,
    )


def text_embed(train_txt, apply_txts, n_components, tag):
    """Fit TF-IDF char_wb(2,4) + SVD on train_txt only; transform each series given."""
    vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4), min_df=20)
    xtr = vec.fit_transform(train_txt)
    k = min(n_components, xtr.shape[1] - 1)
    assert k >= 1, f"{tag}: vocabulary too small for SVD"
    svd = TruncatedSVD(n_components=k, random_state=RS)
    svd.fit(xtr)
    cols = [f"svd_{tag}_{i}" for i in range(k)]
    return [(pd.DataFrame(svd.transform(vec.transform(t)), index=t.index, columns=cols)) for t in apply_txts]


def add_text(base_frame, tr_idx, ap_idx_list, name_txt, dom_txt, with_domain):
    """Return HGB frames for each apply-index set, with fold-fitted text features."""
    name_parts = text_embed(name_txt.loc[tr_idx], [name_txt.loc[i] for i in ap_idx_list], 16, "name")
    dom_parts = (
        text_embed(dom_txt.loc[tr_idx], [dom_txt.loc[i] for i in ap_idx_list], 8, "dom")
        if with_domain
        else [None] * len(ap_idx_list)
    )
    frames = []
    for idx, npart, dpart in zip(ap_idx_list, name_parts, dom_parts, strict=False):
        parts = [base_frame.loc[idx], npart] + ([dpart] if with_domain else [])
        frames.append(pd.concat(parts, axis=1))
    return frames


def main():
    df = pd.read_parquet(harness.WORKING_SET)
    folds = harness.load_folds()
    df = df.merge(
        folds[["campaign_id", "fold", "forward_role", "decidable"]],
        on="campaign_id",
        how="left",
        validate="one_to_one",
    )
    assert df["fold"].notna().all()

    eng = engineer(df)
    y = eng["corroborated"].astype(int)
    x_base = make_hgb_frame(eng)  # EXACTLY CAT_COLS + NUM_COLS

    # Text inputs (both exist at signup -> pass-B legal)
    name_txt = (
        (eng["user_first_name"].fillna("") + " " + eng["user_last_name"].fillna("")).str.strip().str.lower()
    )
    dom_txt = eng["email_domain"].fillna("").str.lower()
    print(f"n={len(eng):,}  positive={y.mean():.4f}")

    variants = {"base+name": False, "base+name+domain": True}
    oof = {v: np.zeros(len(eng)) for v in variants}

    for k in sorted(eng["fold"].unique()):
        tr, va = eng.index[eng["fold"].ne(k)], eng.index[eng["fold"].eq(k)]
        for v, with_dom in variants.items():
            xtr, xva = add_text(x_base, tr, [tr, va], name_txt, dom_txt, with_dom)
            m = make_hgb().fit(xtr, y.loc[tr])
            oof[v][eng.index.get_indexer(va)] = m.predict_proba(xva)[:, 1]
        print(f"fold {k} done")

    print(
        f"\nbaseline (sentinel-aware hgb): roc_auc={BASELINE['roc_auc']} "
        f"decidable_roc_auc={BASELINE['decidable_roc_auc']}"
    )
    results = {}
    for v in variants:
        results[v] = harness.evaluate(y, oof[v], eng["decidable"])
        print(v, results[v])

    # Forward split: vectorizers + SVD fit on the 2025 train side only
    tr = eng.index[eng["forward_role"].eq("train_2025")]
    te = eng.index[eng["forward_role"].eq("test_2026plus")]
    print(
        f"\nforward: train n={len(tr):,} test n={len(te):,} " f"(baseline hgb {BASELINE['forward_roc_auc']})"
    )
    fwd = {}
    for v, with_dom in variants.items():
        xtr, xte = add_text(x_base, tr, [tr, te], name_txt, dom_txt, with_dom)
        m = make_hgb().fit(xtr, y.loc[tr])
        fwd[v] = harness.evaluate(y.loc[te], m.predict_proba(xte)[:, 1])
        print(v, fwd[v])

    out = eng[["user_id", "campaign_id", "corroborated"]].copy()
    out["oof_hgb_name"] = oof["base+name"]
    out["oof_hgb_name_domain"] = oof["base+name+domain"]
    out.to_parquet(HERE / "datasets" / "oof_char_ngram.parquet", index=False)
    print("\nsaved datasets/oof_char_ngram.parquet")


if __name__ == "__main__":
    main()
