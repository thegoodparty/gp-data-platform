"""Collect DATA-2251 sweep verdicts into one results file.

Reads every per-batch verdict CSV in datasets/web_verify_sweep_raw/
(written by the verification agents' outputs, one file per batch, named
<batch>.csv with header verify_id,verdict,evidence_url,source_type,notes),
validates against the manifest, and writes
datasets/web_verify_sweep_results.csv = manifest columns + verdict fields.

Validation is loud, not silent: unknown verify_ids, duplicate verify_ids,
invalid verdicts, and manifest rows still missing a verdict are all printed.

Run: cd analytics && uv run --with pyarrow python projects/legit_candidate_predictor/web_verify_collect.py
"""

from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
D = HERE / "datasets"
RAW = D / "web_verify_sweep_raw"

VALID_VERDICTS = {
    "CONFIRMED",
    "LIKELY",
    "CONTRADICTED",
    "NOT_FOUND",
    "NOT_FOUND_BUDGET",
}
VALID_SOURCE_TYPES = {
    "official_filing",
    "certified_results",
    "ballot",
    "ballotpedia",
    "news",
    "campaign_site",
    "other",
    "",
}


def main() -> None:
    manifest = pd.read_csv(D / "web_verify_sweep_manifest.csv")

    frames = []
    for f in sorted(RAW.glob("*.csv")):
        df = pd.read_csv(f, dtype=str).fillna("")
        df["batch"] = f.stem
        frames.append(df)
    if not frames:
        print("no verdict files in", RAW)
        return
    verdicts = pd.concat(frames, ignore_index=True)

    problems = []
    unknown = set(verdicts["verify_id"]) - set(manifest["verify_id"])
    if unknown:
        problems.append(f"unknown verify_ids: {sorted(unknown)}")
    dupes = verdicts[verdicts["verify_id"].duplicated(keep=False)]
    if len(dupes):
        problems.append(f"duplicate verify_ids: {sorted(dupes['verify_id'].unique())}")
    bad_verdict = verdicts[~verdicts["verdict"].isin(VALID_VERDICTS)]
    if len(bad_verdict):
        problems.append("invalid verdicts: " + str(bad_verdict[["verify_id", "verdict"]].to_dict("records")))
    bad_source = verdicts[~verdicts["source_type"].isin(VALID_SOURCE_TYPES)]
    if len(bad_source):
        problems.append(
            "nonstandard source_type (kept, flagged): "
            + str(bad_source[["verify_id", "source_type"]].to_dict("records"))
        )

    merged = manifest.merge(
        verdicts.drop_duplicates("verify_id", keep="first"),
        on="verify_id",
        how="left",
    )
    merged.to_csv(D / "web_verify_sweep_results.csv", index=False)

    done = merged["verdict"].notna() & (merged["verdict"] != "")
    print(f"verdicts collected: {int(done.sum())}/{len(manifest)}")
    print(
        "by verdict:",
        merged.loc[done, "verdict"].value_counts().to_dict(),
    )
    by_wave = merged.assign(has=done).groupby("wave")["has"].agg(["sum", "count"]).to_dict("index")
    print("by wave:", {k: f"{int(v['sum'])}/{v['count']}" for k, v in by_wave.items()})
    missing_batches = sorted(
        {
            f"F{(int(v[1:]) - 1) // 6 + 1}"
            for v in manifest.loc[
                ~manifest["verify_id"].isin(verdicts["verify_id"]) & (manifest["wave"] == "sweep"),
                "verify_id",
            ]
        },
        key=lambda b: int(b[1:]),
    )
    if missing_batches:
        print(f"batches still missing ({len(missing_batches)}):", missing_batches)
    for p in problems:
        print("PROBLEM:", p)


if __name__ == "__main__":
    main()
