"""DATA-2239 L2/haystaq FULL-BLOCK screen, step 3: pull the surviving candidate columns
for the bridge-resolved users and apply the PIT gates.

Scope (HANDOFF 2026-08-13): the 25-column easy-match family (l2_easy_match /
l2_lean_variant, both washes) was a hand-picked shortlist of a ~1,400-column surface.
This build pulls EVERY column that survived the mechanical triage
(datasets/l2_block_dispositions.csv) and the protected-class proxy screen
(datasets/l2_block_candidates.csv) — no informativeness pre-selection, so the full-block
experiment stays an honest negative if it comes back flat.

Join: datasets/l2_match_bridge.parquet (user_id -> LALVOTERID, frozen 2026-08-12; the
56% working-set reach is ACCEPTED — the result informs DATA-2244 urgency). The bridge
stays the only artifact carrying LALVOTERID; this feature file carries user_id only.
Spine: all users in datasets/l2_features.parquet (whole named-and-stated population),
so l2b_matched is a real 0 for unmatched users, exactly like the easy-match family.

TIME DISCIPLINE (the L2 snapshot is acquired independently of candidate arrival, so
attributes are a-priori; the outcome-shaped channels are gated exactly as
build_l2_match.py):
  - year-indexed vote history (General_/Primary_/AnyElection_/OtherElection_/
    PresidentialPrimary_/PRI_BLT_<year>): cell-masked to NaN when year >= signup year
    (voting/ballot-party in your own election year is contemporaneous with running)
  - registration dates -> years-at-signup, NaN when registration postdates signup
    (registering after signup is often registering TO run)
  - MovedFrom date -> days-since-move at signup, NaN when the move postdates signup
  - FEC donation fields only when the last donation predates signup (cumulative totals
    otherwise include post-signup giving); tri-state donor flag as in build_l2_match.py

Naming: every emitted column is prefixed l2b_ so the block can coexist with (and be
masked against) the easy-match family without collisions.

Leakage discipline unchanged: no ambiguity counts, no name-derived columns (they never
survived triage), match_type stays in the bridge.

Run (needs the step-2 outputs; pull is chunked by columns through the id list):
  cd analytics && uv run --with pyarrow python \
      projects/legit_candidate_predictor/build_l2_block.py
"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from profile import write_profile  # noqa: E402

from intake import write_intake  # noqa: E402

TABLE = "goodparty_data_catalog.dbt.int__l2_nationwide_uniform_w_haystaq"
CHUNK = 240
YEAR_RE = re.compile(r"^(General|Primary|AnyElection|OtherElection|PresidentialPrimary|PRI_BLT)_(\d{4})$")

SIGNUP_SQL = """
select u.user_id, cast(u.user_created_at as date) as signup_date
from goodparty_data_catalog.mart_analytics.users_win_candidacy u
where u.is_latest_version and not u.is_demo
qualify row_number() over (partition by u.user_id
                           order by u.election_date desc nulls last, u.campaign_id desc) = 1
"""


def pull(ids: list[str], cols: list[str]) -> pd.DataFrame:
    from databricks_conn import run_query

    id_lit = ",".join(ids)
    parts = []
    for i in range(0, len(cols), CHUNK):
        chunk = cols[i : i + CHUNK]
        sel = ", ".join(f"t.`{c}`" for c in chunk)
        print(f"  chunk {i // CHUNK + 1}: {len(chunk)} cols ...", flush=True)
        sql = (
            f"with ids as (select explode(split('{id_lit}', ',')) as LALVOTERID) "
            f"select t.LALVOTERID, {sel} from {TABLE} t "
            f"join ids on t.LALVOTERID = ids.LALVOTERID"
        )
        parts.append(run_query(sql).set_index("LALVOTERID"))
    df = parts[0].join(parts[1:]) if len(parts) > 1 else parts[0]
    return df.reset_index()


def _num(s: pd.Series) -> pd.Series:
    """Parse a numeric that may arrive as a string (%/$/comma stripped, None-safe)."""
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")
    cleaned = s.astype(str).str.strip().str.replace(r"[%$,]", "", regex=True)
    return pd.to_numeric(cleaned.where(s.notna()), errors="coerce")


def build(raw: pd.DataFrame, cand: pd.DataFrame, signup: pd.Series) -> pd.DataFrame:
    """raw: one row per matched user (bridge order), candidate columns as pulled.
    signup: signup_date aligned to raw.index. Returns the l2b_-prefixed feature frame."""
    signup_year = signup.dt.year
    out = pd.DataFrame(index=raw.index)

    date_cols = {
        "Voters_CalculatedRegDate",
        "Voters_OfficialRegDate",
        "Voters_MovedFrom_Date",
        "FECDonors_LastDonationDate",
    }
    fec_gated = {
        "FECDonors_NumberOfDonations",
        "FECDonors_TotalDonationsAmount",
        "FECDonors_AvgDonation",
        "FECDonors_AvgDonation_Range",
        "FECDonors_TotalDonationsAmt_Range",
    }

    if "FECDonors_LastDonationDate" in raw.columns:
        last_don = pd.to_datetime(raw["FECDonors_LastDonationDate"], errors="coerce")
    else:  # column subset without FEC (e.g. the proxy-cost diagnostic)
        last_don = pd.Series(pd.NaT, index=raw.index)
    don_gate = last_don.notna() & (last_don <= signup)

    for _, r in cand.iterrows():
        c = r.column_name
        m = YEAR_RE.match(c)
        if m:  # year-indexed: mask own-and-later years cell-wise
            # L2 semantics: vote history is True/NULL — for a MATCHED voter, NULL means
            # "did not vote", a genuine 0 (accepted on the easy-match family 2026-08-12).
            # Unmatched users never reach this frame; they stay NaN via the spine merge.
            year = int(m.group(2))
            gate = signup_year > year
            if m.group(1) == "PRI_BLT":
                s = raw[c].map(lambda x: x.strip() if isinstance(x, str) else x)
                s = pd.Series(s, index=raw.index).replace("", np.nan)
                out[f"l2b_{c}"] = s.fillna("NONE").where(gate)  # NONE = no primary ballot
            else:
                s = raw[c].fillna(False).astype(bool).astype(float)
                out[f"l2b_{c}"] = s.where(gate)
            continue
        if c in date_cols:
            d = pd.to_datetime(raw[c], errors="coerce")
            if c == "Voters_MovedFrom_Date":
                days = (signup - d).dt.days
                out["l2b_days_since_move_at_signup"] = days.where(days >= 0)
            elif c == "FECDonors_LastDonationDate":
                yrs = (signup - d).dt.days / 365.25
                out["l2b_fec_years_since_last_donation"] = yrs.where(don_gate)
            else:
                yrs = (signup - d).dt.days / 365.25
                name = (
                    "l2b_years_registered_at_signup"
                    if c == "Voters_CalculatedRegDate"
                    else "l2b_years_official_reg_at_signup"
                )
                out[name] = yrs.where(yrs >= 0)
            continue
        if c in fec_gated:
            if r.data_type == "STRING" and not c.endswith("_Range"):
                out[f"l2b_{c}"] = _num(raw[c]).where(don_gate)
            elif c.endswith("_Range"):
                out[f"l2b_{c}"] = raw[c].astype("object").where(don_gate)
            else:
                out[f"l2b_{c}"] = _num(raw[c]).where(don_gate)
            continue
        # a-priori snapshot attribute
        if r.data_type == "DOUBLE":
            out[f"l2b_{c}"] = pd.to_numeric(raw[c], errors="coerce")
        elif r.data_type == "BOOLEAN":
            out[f"l2b_{c}"] = raw[c].astype("float")
        else:
            s = raw[c].astype("object")
            s = s.where(s.notna(), np.nan)
            stripped = pd.Series(s, index=raw.index).astype(str).str.strip()
            s = stripped.where(stripped.ne("") & stripped.ne("nan") & stripped.ne("None"))
            conv = _num(raw[c])
            # numeric-in-a-string (square footage, HH counts, ages, performance %s)
            if conv.notna().sum() >= 0.98 * s.notna().sum() and conv.notna().any():
                out[f"l2b_{c}"] = conv
            elif "Date" in c:
                # remaining a-priori vendor dates (e.g. Home_Purchase_Date) -> year,
                # mirroring the proxy screen's treatment; raw strings are 1000s of levels
                out[f"l2b_{c}"] = pd.to_numeric(s.str.extract(r"((?:19|20)\d{2})")[0], errors="coerce")
            elif s.dropna().nunique() == 1:
                # single-level presence flag ('Y'/NULL consumer data): for a MATCHED
                # person, absence of the flag is a genuine 0, not unknown
                out[f"l2b_{c}"] = s.notna().astype(float)
            else:
                out[f"l2b_{c}"] = s
        continue

    out["l2b_fec_donor_pre_signup"] = np.where(don_gate, 1.0, np.where(last_don.isna(), 0.0, np.nan))
    return out


if __name__ == "__main__":
    from databricks_conn import run_query

    cand = pd.read_csv(HERE / "datasets" / "l2_block_candidates.csv")
    cand = cand[~cand.proxy_dropped].reset_index(drop=True)
    print(f"candidate columns: {len(cand)}")

    bridge = pd.read_parquet(HERE / "datasets" / "l2_match_bridge.parquet")
    ids = bridge["LALVOTERID"].dropna().astype(str).unique().tolist()
    print(f"bridge users: {len(bridge):,}  distinct LALVOTERID: {len(ids):,}")

    # local raw cache (user_id-keyed, LALVOTERID dropped: the bridge stays the only
    # artifact carrying the voter id) so gate/encoding iterations skip the warehouse
    cache = HERE / "datasets" / "_l2_block_raw.parquet"
    if cache.exists():
        j = pd.read_parquet(cache)
        print(f"raw cache hit: {len(j):,} rows")
    else:
        raw = pull(ids, cand.column_name.tolist())
        print(f"pulled: {len(raw):,} rows")
        # a voter can resolve for two users (rare); keep the bridge grain via merge
        j = bridge[["user_id", "LALVOTERID"]].merge(raw, on="LALVOTERID", how="left")
        assert len(j) == len(bridge)
        sd = run_query(SIGNUP_SQL)
        sd["signup_date"] = pd.to_datetime(sd["signup_date"])
        j = j.merge(sd, on="user_id", how="left", validate="one_to_one")
        assert j["signup_date"].notna().all(), "every bridge user needs a signup date"
        j = j.drop(columns=["LALVOTERID"])
        j.to_parquet(cache, index=False)

    feats = build(j.drop(columns=["user_id", "signup_date"]), cand, j["signup_date"])
    feats.insert(0, "user_id", j["user_id"].to_numpy())

    # spine: whole named-and-stated population so l2b_matched is a real 0/1
    spine = pd.read_parquet(HERE / "datasets" / "l2_features.parquet")[["user_id"]]
    full = spine.merge(feats, on="user_id", how="left")
    full.insert(1, "l2b_matched", full["user_id"].isin(set(feats["user_id"])).astype(int))

    # safety net: HGB rejects categoricals above 255 levels; nothing should reach here
    too_wide = [c for c in full.columns if full[c].dtype == object and full[c].nunique(dropna=True) > 255]
    if too_wide:
        print(f"dropping {len(too_wide)} over-255-level categoricals: {too_wide}")
        full = full.drop(columns=too_wide)

    # drop columns the gates left empty (e.g. future vote-history years)
    value_cols = [c for c in full.columns if c not in ("user_id", "l2b_matched")]
    empty = [c for c in value_cols if full[c].notna().sum() == 0]
    if empty:
        print(
            f"dropping {len(empty)} all-NaN-after-gating columns: {empty[:12]}"
            f"{' ...' if len(empty) > 12 else ''}"
        )
        full = full.drop(columns=empty)

    path = HERE / "datasets" / "l2_block_features.parquet"
    full.to_parquet(path, index=False)
    n_feat = full.shape[1] - 1
    print(
        f"saved -> {path}  ({len(full):,} rows x {n_feat} features, "
        f"matched {full.l2b_matched.mean():.1%})"
    )

    # ~100 stratified sample rows for eyeballing (never a full export)
    strat = full.groupby("l2b_matched", group_keys=False).apply(
        lambda g: g.sample(n=min(len(g), 50 if g.name == 0 else 60), random_state=7)
    )
    strat.to_csv(HERE / "datasets" / "l2_block_features_sample.csv", index=False)

    write_profile(
        full,
        "l2_block_features",
        note="L2/haystaq FULL BLOCK (post triage + protected-class proxy screen), "
        "attached through the frozen easy-match bridge (56% ws reach "
        "accepted). Year-indexed vote history cell-gated to years strictly "
        "before signup; registration/move/FEC date-gated as build_l2_match. "
        "All other attributes a-priori snapshot. No identifiers beyond "
        "user_id; LALVOTERID stays in the bridge.",
    )
    write_intake(
        full,
        "l2_block_features",
        key="user_id",
        source=TABLE,
        join="user_id -> l2_match_bridge (frozen 2026-08-12) -> LALVOTERID",
        pit_class="B with current-snapshot caveat; year-indexed columns cell-gated "
        "pre-signup-year; reg/move/FEC date-gated. Protected classes + "
        "proxies removed label-free upstream (l2_proxy_screen.csv). "
        "RESERVE candidates: registration/vote-history block (claims).",
    )
