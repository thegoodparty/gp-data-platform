"""DATA-2239 stage 7, wave 3: the deadline/arrivals-derived variables (DATA-2312).

Held back until the DATA-2235 rebuild so they are built once on restated data instead of
twice. All three come from DERIVED_VARIABLES.md and are label-free.

True ratios (class 1):
- 1.3 drv_crowding_per_seat = n_br_candidacies_by_deadline / pos_seats. Field competitiveness.
  Raw arrivals conflate "big field" with "many seats": 6 candidates for 5 seats is an empty
  race, 6 for 1 is a scrum.
- 1.4 drv_signup_lateness_fraction = (signup - filing_window_start) / window length. Where in
  the filing window they arrive. Sharper than raw days-to-deadline, which cannot tell a late
  arrival from a short window.

Cohort-relative (class 3):
- 3.3 drv_crowding_vs_race_norm = crowding_per_seat / trailing median for (state, office_type).
  Crowded for this KIND of race, not crowded in absolute terms.

SENTINEL DISCIPLINE (the standing caution in DERIVED_VARIABLES: a ratio inherits the leakage
class of its worst ingredient). n_br_candidacies_by_deadline is only meaningful when a deadline
is known: with no deadline the SQL's CASE can never fire, so 0 means "unknown", not "nobody
filed". Crowding is therefore NaN wherever the deadline is missing, mirroring mvp_model.engineer.
Skipping that turns ~20% of rows into a fake 0.0 crowding and rebuilds the exact sentinel bug
fixed on 2026-08-06.

Arrivals are OPPONENTS-ONLY as of the DATA-2312 rebuild, so crowding counts the field a
candidate faces rather than the field including themselves.

PIT: trailing medians use STRICTLY EARLIER signups only (expanding median, self and same-day
peers excluded, minimum 30 prior observations else NaN). Lateness is computable at signup.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/build_derived_wave3.py
"""

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from profile import write_profile  # noqa: E402

from build_derived_wave1 import trailing_median  # noqa: E402
from intake import write_intake  # noqa: E402


def build(ws: pd.DataFrame, br: pd.DataFrame) -> pd.DataFrame:
    df = ws.merge(br[["br_position_id", "pos_seats"]], on="br_position_id", how="left")
    assert len(df) == len(ws), "position merge fanned out"
    df["signup_date"] = pd.to_datetime(df["signup_date"])
    deadline = pd.to_datetime(df["filing_deadline"])
    start = pd.to_datetime(df["filing_window_start"])

    out = pd.DataFrame({"campaign_id": df["campaign_id"]})

    # 1.3 crowding per seat. NaN when the deadline is unknown (see SENTINEL DISCIPLINE).
    seats = pd.to_numeric(df["pos_seats"], errors="coerce")
    seats = seats.where(seats > 0)
    arrivals = pd.to_numeric(df["n_br_candidacies_by_deadline"], errors="coerce")
    arrivals = arrivals.where(deadline.notna())
    crowding = arrivals / seats
    out["drv_crowding_per_seat"] = crowding

    # 1.4 where in the filing window the signup lands. Deliberately unclipped: below 0 is a
    # signup before the window opened and above 1 is one after it shut, both real and both
    # meaningful. NaN when either endpoint is missing or the window has no width.
    span = (deadline - start).dt.days
    span = span.where(span > 0)
    out["drv_signup_lateness_fraction"] = (df["signup_date"] - start).dt.days / span

    # 3.3 crowding relative to the trailing norm for this kind of race.
    #
    # DERIVED_VARIABLES specifies a RATIO to the trailing median. That is undefined here: 56%
    # of races have no opponent arriving by the deadline, so the trailing median is itself 0
    # almost everywhere and the ratio survived on 1.1% of rows. Using the DIFFERENCE instead,
    # which is the same choice wave 1 made for days_to_election_vs_office_norm (subtraction)
    # against voter_count_vs_office_norm (log ratio): subtract when the quantity is count-like
    # with mass at zero, divide when it is a strictly positive magnitude.
    df["_crowd"] = crowding
    df["_race_kind"] = (
        df["campaign_state"].astype(str).str.upper().str.strip()
        + "|"
        + df["office_type"].astype(str).str.upper().str.strip()
    )
    norm = trailing_median(df, "_crowd", "_race_kind")
    out["drv_crowding_vs_race_norm"] = crowding - norm
    return out


if __name__ == "__main__":
    ws = pd.read_parquet(HERE / "datasets" / "working_set.parquet")
    br = pd.read_parquet(HERE / "datasets" / "br_position_features.parquet")
    assert ws["campaign_id"].is_unique, "campaign_id must be the key"
    print(f"working set rows: {len(ws):,}")

    feats = build(ws, br)
    for c in feats.columns[1:]:
        s = feats[c]
        print(
            f"  {c}: fill {s.notna().mean():.1%}  median {s.median():.4g}  "
            f"p10 {s.quantile(.1):.4g}  p90 {s.quantile(.9):.4g}"
        )

    # The sentinel rule is the thing most likely to be silently undone by a later edit, so
    # assert it rather than trusting the fill rate to look wrong if it breaks.
    no_deadline = pd.to_datetime(ws["filing_deadline"]).isna()
    assert (
        feats.loc[no_deadline.to_numpy(), "drv_crowding_per_seat"].isna().all()
    ), "crowding present on a deadline-missing row: sentinel discipline broken"
    print(f"sentinel check OK: crowding NaN on all {no_deadline.sum():,} deadline-missing rows")

    out = HERE / "datasets" / "derived_wave3_features.parquet"
    feats.to_parquet(out, index=False)
    print(f"saved -> {out}")

    write_profile(
        feats,
        "derived_wave3_features",
        note="Stage-7 wave 3: crowding per seat, signup lateness within the filing "
        "window, and crowding vs the trailing (state, office_type) norm. Built "
        "post-DATA-2312 on opponents-only arrivals and restated deadlines; "
        "crowding is NaN wherever the deadline is unknown.",
    )
    write_intake(
        feats,
        "derived_wave3_features",
        key="campaign_id",
        source="working_set.parquet (ws b04cdd5ac4b1) + br_position_features.parquet",
        join="campaign_id grain; seats via br_position_id, norms via (state, "
        "office_type, trailing signup window)",
        pit_class="B (arrivals as of the filing deadline; cohort stats from prior "
        "signups only; lateness known at signup)",
    )
