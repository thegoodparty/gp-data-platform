"""Build blind web-verification batches for the DATA-2251 full sweep.

Population: the 630 cleanlab high-confidence flagged negatives, minus anyone
already verified in the 2026-08-07 pilot (30) or stratified run (150).
Also emits a mop-up wave: stratified rows that came back NOT_FOUND_BUDGET
(access-blocked, not adjudicated), keeping their original S verify_ids.

Outputs (all under datasets/):
  web_verify_sweep_batches/F<N>.csv   blind 6-row batches (F-prefixed ids)
  web_verify_sweep_batches/M<N>.csv   mop-up batches (original S ids)
  web_verify_sweep_manifest.csv       join-back metadata, one row per verify_id

Blind protocol: batch files carry only verify_id, name, state, zip, office,
position_type, election_date. No scores, no flags, no group membership.

Run: cd analytics && uv run --with pyarrow python projects/legit_candidate_predictor/web_verify_make_sweep_batches.py
"""

from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
D = HERE / "datasets"
OUT = D / "web_verify_sweep_batches"
BATCH_SIZE = 6

BLIND_COLS = [
    "verify_id",
    "first",
    "last",
    "state",
    "zip",
    "office",
    "position_type",
    "election_date",
]


def _zip5(z: object) -> str:
    """Normalize zips: floats like 50005.0 and zip+4 like 44133-3744 -> 5 digits."""
    if pd.isna(z):
        return ""
    s = str(z).split("-")[0].split(".")[0].strip()
    return s.zfill(5) if s.isdigit() else s


def main() -> None:
    cl = pd.read_parquet(D / "cleanlab_mislabeled_negatives.parquet")
    ws = pd.read_parquet(D / "working_set.parquet")
    pilot = pd.read_csv(D / "web_verify_results.csv")
    strat = pd.read_csv(D / "web_verify_stratified_results.csv")

    high = cl[cl["high_conf"]].merge(
        # campaign_state / election_level / br_normalized_position_type already
        # come from the cleanlab frame; pull only the missing fields from ws.
        ws[
            [
                "campaign_id",
                "user_id",
                "user_first_name",
                "user_last_name",
                "user_zip",
                "office_type",
                "general_election_date",
            ]
        ],
        on=["campaign_id", "user_id"],
        how="left",
    )
    assert len(high) == int(cl["high_conf"].sum()), "join changed row count"

    done_campaigns = set(pilot["campaign_id"]) | set(strat["campaign_id"])
    sweep = high[~high["campaign_id"].isin(done_campaigns)].copy()
    n_deduped = len(high) - len(sweep)

    sweep = sweep.sort_values(["campaign_state", "campaign_id"]).reset_index(drop=True)
    sweep["verify_id"] = [f"F{i + 1:03d}" for i in range(len(sweep))]

    mopup = strat[strat["verdict"] == "NOT_FOUND_BUDGET"].copy()

    OUT.mkdir(exist_ok=True)
    for old in OUT.glob("*.csv"):
        old.unlink()

    def blind_frame(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "verify_id": df["verify_id"],
                "first": df["user_first_name"],
                "last": df["user_last_name"],
                "state": df["campaign_state"],
                "zip": df["user_zip"].map(_zip5),
                "office": df["office_type"],
                "position_type": df["br_normalized_position_type"],
                "election_date": df["general_election_date"],
            }
        )

    for prefix, frame in (("F", blind_frame(sweep)), ("M", blind_frame(mopup))):
        for b, start in enumerate(range(0, len(frame), BATCH_SIZE), start=1):
            frame.iloc[start : start + BATCH_SIZE].to_csv(OUT / f"{prefix}{b}.csv", index=False)

    manifest_cols = [
        "verify_id",
        "campaign_id",
        "user_id",
        "oof_hgb",
        "cleanlab_issue",
        "high_conf",
        "user_first_name",
        "user_last_name",
        "campaign_state",
        "user_zip",
        "election_level",
        "office_type",
        "br_normalized_position_type",
        "general_election_date",
    ]
    manifest = sweep[[c for c in manifest_cols if c in sweep.columns]].copy()
    manifest["wave"] = "sweep"
    mop_manifest = mopup[[c for c in manifest_cols if c in mopup.columns]].copy()
    mop_manifest["wave"] = "mopup"
    pd.concat([manifest, mop_manifest], ignore_index=True).to_csv(
        D / "web_verify_sweep_manifest.csv", index=False
    )

    n_f = len(list(OUT.glob("F*.csv")))
    n_m = len(list(OUT.glob("M*.csv")))
    print(f"high-conf total: {len(high)}")
    print(f"already verified (pilot+stratified overlap): {n_deduped}")
    print(f"sweep rows: {len(sweep)} -> {n_f} F-batches of {BATCH_SIZE}")
    print(f"mop-up rows: {len(mopup)} -> {n_m} M-batches")
    print(f"manifest: {len(manifest) + len(mop_manifest)} rows")


if __name__ == "__main__":
    main()
