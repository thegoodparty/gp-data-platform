"""DATA-2239 L2 screen DIAGNOSTIC: what did the protected-class proxy screen cost in AUC?

Tristan's question (2026-08-13): if the 150 proxy-dropped columns are added back — the
PROXIES only, never the protected-class columns themselves — does the model improve?

Purpose is documentation, not candidacy: flat means the fairness screen was free;
positive means we knowingly gave that lift up and the number is on the record. The
output parquet is a DIAGNOSTIC artifact and must not be fed to any serving composition.

Builds datasets/l2_block_plus_proxies.parquet = the 791 accepted features + the 150
proxy columns pulled through the same bridge with the same PIT gates, then run:
  exp_family.py --features l2_block_plus_proxies.parquet --key user_id \
      --name "l2_block_plus_proxies (diagnostic)"

Run:  cd analytics && uv run --with pyarrow python \
        projects/legit_candidate_predictor/diag_l2_proxy_cost.py
"""

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from build_l2_block import build, pull  # noqa: E402

if __name__ == "__main__":
    cand = pd.read_csv(HERE / "datasets" / "l2_block_candidates.csv")
    dropped = cand[cand.proxy_dropped].reset_index(drop=True)
    print(f"proxy-dropped columns to pull: {len(dropped)}")

    bridge = pd.read_parquet(HERE / "datasets" / "l2_match_bridge.parquet")
    ids = bridge["LALVOTERID"].dropna().astype(str).unique().tolist()

    cache = HERE / "datasets" / "_l2_proxycols_raw.parquet"
    if cache.exists():
        j = pd.read_parquet(cache)
    else:
        raw = pull(ids, dropped.column_name.tolist())
        j = bridge[["user_id", "LALVOTERID"]].merge(raw, on="LALVOTERID", how="left")
        assert len(j) == len(bridge)
        # signup dates ride along in the main raw cache
        sd = pd.read_parquet(HERE / "datasets" / "_l2_block_raw.parquet", columns=["user_id", "signup_date"])
        j = j.merge(sd, on="user_id", how="left", validate="one_to_one")
        assert j["signup_date"].notna().all()
        j = j.drop(columns=["LALVOTERID"])
        j.to_parquet(cache, index=False)

    feats = build(j.drop(columns=["user_id", "signup_date"]), dropped, pd.to_datetime(j["signup_date"]))
    feats = feats.drop(columns=["l2b_fec_donor_pre_signup"], errors="ignore")  # dup
    feats.insert(0, "user_id", j["user_id"].to_numpy())

    accepted = pd.read_parquet(HERE / "datasets" / "l2_block_features.parquet")
    both = accepted.merge(feats, on="user_id", how="left", validate="one_to_one")
    empty = [c for c in both.columns if both[c].notna().sum() == 0]
    if empty:
        print(f"dropping {len(empty)} empty after gating: {empty}")
        both = both.drop(columns=empty)
    out = HERE / "datasets" / "l2_block_plus_proxies.parquet"
    both.to_parquet(out, index=False)
    print(
        f"saved -> {out}  ({both.shape[0]:,} rows x {both.shape[1] - 1} features; "
        f"DIAGNOSTIC ONLY — includes protected-class proxies)"
    )
