"""DATA-2239 L2/haystaq FULL-BLOCK screen, step 2: label-free protected-class proxy screen.

DECIDED 2026-08-13: gender / ethnicity / religion / language are excluded a priori (both
model tiers), AND any strong proxy of them must go too ("an Elle subscription must not act
as a gender proxy"). This screen predicts each protected column from each surviving
candidate column on L2 data itself — no corroboration label anywhere near it.

Method
  Sample: deterministic hash sample of int__l2_nationwide_uniform_w_haystaq,
    pmod(hash(LALVOTERID), 2000) = 0  ->  ~106k of 212.6M rows, nationwide.
    Reproducible, label-free, and not conditioned on our user base.
  Targets (the protected columns themselves, pulled ONLY into this screening sample,
    never into the feature pull):
      gender    = Voters_Gender
      ethnicity = EthnicGroups_EthnicGroup1Desc  (L2's coarse groups)
      religion  = ConsumerData_Religion_Code
      language  = ConsumerData_Language_Code
  Statistic per candidate column: bin it (numeric -> deciles; categorical -> levels;
    missing is its own level), then
      proxy_strength = max over (level with n >= MIN_SUPPORT, target class with
                       base rate >= 1%) of |P(class | level) - P(class)|
    i.e. "does knowing this column's value move any protected-class posterior".
    The max over a missing-level is reported separately (missingness_shift) because a
    coverage pattern is a different conversation than a value pattern.
  Threshold (recommendation, documented for the decision gate): DROP at
    proxy_strength >= 0.30; sensitivity counts reported at 0.20 and 0.25.

Also computed from the same sample (mechanical-triage completion):
  - dead/constant columns (all-null, or single value with <0.1% missing)
  - cardinality check: kept STRING columns with > 60 levels get reclassified out

Outputs
  datasets/l2_proxy_screen.csv      per column x target: shift, level, class, support
  datasets/l2_block_candidates.csv  final candidate list post-screen (with time notes)
  datasets/_l2_proxy_sample.parquet local cache of the screening sample (LALVOTERID
                                    dropped after pull; protected cols stay ONLY here)

Run (pull is chunked, ~5 queries):
  cd analytics && uv run --with pyarrow python \
      projects/legit_candidate_predictor/screen_l2_proxy.py [--from-cache]
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))

TABLE = "goodparty_data_catalog.dbt.int__l2_nationwide_uniform_w_haystaq"
HASH_FILTER = "pmod(hash(LALVOTERID), 2000) = 0"
CHUNK = 240
MIN_SUPPORT = 200
BASE_RATE_MIN = 0.01
DROP_AT = 0.30

TARGETS = {
    "gender": "Voters_Gender",
    "ethnicity": "EthnicGroups_EthnicGroup1Desc",
    "religion": "ConsumerData_Religion_Code",
    "language": "ConsumerData_Language_Code",
}

SAMPLE_CACHE = HERE / "datasets" / "_l2_proxy_sample.parquet"


def pull_sample(cands: list[str]) -> pd.DataFrame:
    from databricks_conn import run_query

    cols = list(dict.fromkeys(list(TARGETS.values()) + cands))
    parts = []
    for i in range(0, len(cols), CHUNK):
        chunk = cols[i : i + CHUNK]
        sel = ", ".join(f"`{c}`" for c in ["LALVOTERID", *chunk])
        print(f"  chunk {i // CHUNK + 1}: {len(chunk)} cols ...", flush=True)
        part = run_query(f"select {sel} from {TABLE} where {HASH_FILTER}")
        parts.append(part.set_index("LALVOTERID"))
    df = pd.concat(parts, axis=1, join="inner").reset_index(drop=True)  # id dropped
    return df


def bin_column(s: pd.Series) -> pd.Series:
    """Level-ize a candidate: deciles for numerics, categories otherwise, NaN kept."""
    if pd.api.types.is_bool_dtype(s):
        lev = s.astype("object")
    elif pd.api.types.is_numeric_dtype(s):
        nun = s.nunique(dropna=True)
        if nun == 0:
            return pd.Series(np.nan, index=s.index, dtype="object")
        lev = s.astype("object") if nun <= 12 else pd.qcut(s, 10, duplicates="drop").astype("object")
    else:
        lev = s.map(lambda x: (x.strip() or np.nan) if isinstance(x, str) else x)
    lev = pd.Series(lev, index=s.index)
    # string labels throughout: boolean/interval/int levels break .loc on mixed indexes
    return lev.map(lambda x: "__missing__" if pd.isna(x) else str(x))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-cache", action="store_true")
    args = ap.parse_args()

    disp = pd.read_csv(HERE / "datasets" / "l2_block_dispositions.csv")
    cands = disp[disp.disposition == "keep"].column_name.tolist()

    if args.from_cache and SAMPLE_CACHE.exists():
        df = pd.read_parquet(SAMPLE_CACHE)
    else:
        df = pull_sample(cands)
        df.to_parquet(SAMPLE_CACHE, index=False)
    print(f"screening sample: {len(df):,} rows x {df.shape[1]} cols")

    # numeric-looking strings -> numeric (ages, counts, amounts; %/$/comma stripped;
    # denominator = truly non-null originals, NOT astype(str) which turns None into 'None')
    for c in cands:
        if df[c].dtype == object:
            cleaned = df[c].astype(str).str.strip().str.replace(r"[%$,]", "", regex=True)
            conv = pd.to_numeric(cleaned.where(df[c].notna()), errors="coerce")
            orig_n = int(df[c].replace("", np.nan).notna().sum())
            if orig_n and conv.notna().sum() >= 0.98 * orig_n:
                df[c] = conv
        if "Date" in c or c.endswith("RegDate"):
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.year

    # ---- target frames (levels with >=1% base rate only)
    tgt = {}
    for tname, tcol in TARGETS.items():
        t = df[tcol].astype("object").str.strip().replace("", np.nan)
        vc = t.value_counts(normalize=True)
        keep_classes = vc[vc >= BASE_RATE_MIN].index
        t = t.where(t.isin(keep_classes))
        tgt[tname] = t
        print(f"target {tname} ({tcol}): fill {t.notna().mean():.1%}, " f"classes {list(keep_classes)[:8]}")

    rows, dead, high_card = [], [], []
    for c in cands:
        s = df[c]
        nun = s.nunique(dropna=True)
        fill = s.replace("", np.nan).notna().mean()
        if nun == 0 or (nun == 1 and fill > 0.999):
            dead.append(c)
            continue
        if df[c].dtype == object and nun > 60:
            high_card.append((c, nun))
            continue
        lev = bin_column(s)
        for tname, t in tgt.items():
            mask = t.notna()
            lv, tv = lev[mask], t[mask]
            base = tv.value_counts(normalize=True)
            ct = pd.crosstab(lv, tv)
            n_lev = ct.sum(axis=1)
            ct = ct.loc[n_lev >= MIN_SUPPORT]
            if ct.empty:
                continue
            post = ct.div(ct.sum(axis=1), axis=0)
            shift = (post - base).abs()
            val_shift = shift.drop(index="__missing__", errors="ignore")
            if not val_shift.empty:
                mx = val_shift.stack().idxmax()
                rows.append(
                    {
                        "column": c,
                        "target": tname,
                        "kind": "value",
                        "shift": float(val_shift.loc[mx]),
                        "level": str(mx[0])[:60],
                        "class": mx[1],
                        "level_n": int(n_lev.loc[mx[0]]),
                    }
                )
            if "__missing__" in shift.index:
                m = shift.loc["__missing__"]
                rows.append(
                    {
                        "column": c,
                        "target": tname,
                        "kind": "missingness",
                        "shift": float(m.max()),
                        "level": "__missing__",
                        "class": m.idxmax(),
                        "level_n": int(n_lev.loc["__missing__"]),
                    }
                )

    res = pd.DataFrame(rows)
    res.to_csv(HERE / "datasets" / "l2_proxy_screen.csv", index=False)

    val = res[res.kind == "value"]
    strength = val.groupby("column")["shift"].max()
    dropped = strength[strength >= DROP_AT]
    print(f"\ndead/constant: {len(dead)}   high-cardinality strings reclassified: " f"{len(high_card)}")
    for c, n in high_card:
        print(f"  {c}: {n} levels")
    print(f"\nproxy screen at {DROP_AT}: DROP {len(dropped)} of {strength.size} " f"screenable candidates")
    for thr in (0.20, 0.25):
        print(f"  sensitivity: {int((strength >= thr).sum())} would drop at {thr}")
    top = val.sort_values("shift", ascending=False).head(25)
    print("\ntop value-proxies:")
    print(top[["column", "target", "shift", "level", "class", "level_n"]].to_string(index=False))
    miss = res[res.kind == "missingness"].groupby("column")["shift"].max()
    print(
        f"\nmissingness-shift >= {DROP_AT} (reported, not auto-dropped): " f"{int((miss >= DROP_AT).sum())}"
    )
    print(miss[miss >= DROP_AT].sort_values(ascending=False).head(15).to_string())

    final = disp[disp.disposition == "keep"].copy()
    final = final[~final.column_name.isin(dead)]
    final = final[~final.column_name.isin([c for c, _ in high_card])]
    final["proxy_strength"] = final.column_name.map(strength)
    final["missingness_shift"] = final.column_name.map(miss)
    final["proxy_dropped"] = final.column_name.isin(dropped.index)
    final.to_csv(HERE / "datasets" / "l2_block_candidates.csv", index=False)
    kept = final[~final.proxy_dropped]
    print(
        f"\nfinal candidates: {len(kept)} "
        f"(from 961 triaged: -{len(dead)} dead, -{len(high_card)} high-card, "
        f"-{len(dropped)} proxy)"
    )
    print("dropped as proxies:")
    for c in dropped.sort_values(ascending=False).index:
        r = val[val.column == c].sort_values("shift", ascending=False).iloc[0]
        print(f"  {c}: {r['shift']:.2f} shift on {r['target']}/{r['class']}")


if __name__ == "__main__":
    main()
