"""Cuts for Win Pro usage by votes needed to win (DATA-2809). Pure pandas over datasets/.

Run from analytics/ to print every table:
    uv run --with pyarrow python projects/win_pro_votes_needed/analyze.py
The notebook imports the same functions.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA = HERE / "datasets"

BANDS = ["under 1,000", "1,000-2,499", "2,500-9,999", "10,000+", "unknown"]
EDGES = [0, 1000, 2500, 10000, np.inf]
AREAS = ["outreach", "voter_data", "ai", "story", "opponents"]
AREA_NAMES = {
    "outreach": "Outreach",
    "voter_data": "Voter data",
    "ai": "AI assistant",
    "story": "Story and issues",
    "opponents": "Opponent research",
}
CHANNELS = ["texting", "robocall", "social", "door_knocking", "phone_banking", "unattributed"]
BUNDLE_TEXTS = 5000
MIN_N = 30  # below this a band shows counts only


def load() -> pd.DataFrame:
    ws = pd.read_parquet(DATA / "working_set.parquet")
    ad = pd.read_parquet(DATA / "area_days.parquet")
    ws["ged"] = pd.to_datetime(ws["ged"])
    ws["user_created_at"] = pd.to_datetime(ws["user_created_at"])
    # Spark decimals arrive as object dtype; numpy math needs floats.
    for c in ["outreach_usd", "wn_override", "wn_race", "wn_position", "voter_count"]:
        ws[c] = pd.to_numeric(ws[c], errors="coerce").astype(float)
    run_ts = pd.Timestamp(ws["run_ts"].iloc[0])

    # gp-api's order: override district, then the picked race, then the position.
    live = np.where(
        ws["has_override"].fillna(False), ws["wn_override"], ws["wn_race"].fillna(ws["wn_position"])
    )
    ws["votes_needed"] = pd.Series(live, index=ws.index)
    ws["wn_source"] = np.select(
        [
            ws["has_override"].fillna(False) & ws["wn_override"].notna(),
            ~ws["has_override"].fillna(False) & ws["wn_race"].notna(),
            ~ws["has_override"].fillna(False) & ws["wn_position"].notna(),
        ],
        ["override district", "picked race", "position"],
        "none",
    )
    ws["band"] = pd.cut(ws["votes_needed"], EDGES, right=False, labels=BANDS[:-1]).astype(str)
    ws.loc[ws["votes_needed"].isna(), "band"] = "unknown"
    ws["band"] = pd.Categorical(ws["band"], BANDS, ordered=True)

    days = ad.pivot_table(index="user_id", columns="area", values="action_days", aggfunc="sum").reindex(
        columns=AREAS
    )
    for a in AREAS:
        ws[f"days_{a}"] = ws["user_id"].map(days[a]).fillna(0)
        ws[f"acted_{a}"] = ws[f"days_{a}"] > 0

    ws["signup_era"] = np.where(ws["user_created_at"] >= "2026-07-01", "from Jul 2026", "before Jul 2026")
    ws["early_election"] = ws["ged"] < "2026-07-15"
    ws["future_election"] = ws["ged"] > run_ts
    # A texter is anyone with a sent text. Some sends carry no billable count (Amplitude legs, a few p2p rows),
    # so volume is measured only where the count is known; counting texters by known volume missed 66 people.
    ws["texted"] = ws["sends_texting"] > 0
    ws["texts_known"] = ws["texts_sent"] > 0
    ws["texts_unknown"] = ws["texted"] & ~ws["texts_known"]
    ws["tried_text_only"] = ~ws["texted"] & (ws["p2p_started"] > 0)
    ws["no_outreach"] = (ws["sends_total"] == 0) & (ws["p2p_started"] == 0)
    ws["activated"] = ws["is_activated"].fillna(False).astype(bool)
    ws["texts_per_vote"] = ws["texts_sent"].where(ws["texts_known"]) / ws["votes_needed"]
    ws["over_bundle"] = ws["texts_sent"] > BUNDLE_TEXTS
    # Texters with no known count could be over too: the upper bound counts them all as over.
    ws["over_bundle_upper"] = ws["over_bundle"] | ws["texts_unknown"]
    # Before Jul 15 the DATA-2690 action events record nothing in outreach, story or opponent
    # research (older event names), though 47 of those users texted. Feature cuts skip them.
    ws["feature_measured"] = ~ws["early_election"]
    ws["any_send"] = ws["sends_total"] > 0
    ws["paid"] = ws["outreach_usd"] > 0
    ws["office_level"] = ws["election_level"].fillna("unknown").str.lower()
    ws["pro_status"] = np.where(ws["pro_now"], "Pro today", "canceled after election")
    return ws


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (np.nan, np.nan)
    p = k / n
    d = 1 + z**2 / n
    c = (p + z**2 / (2 * n)) / d
    h = z * np.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / d
    return (c - h, c + h)


def rate(df: pd.DataFrame, flag: str, by: str = "band") -> pd.DataFrame:
    """'x of n (p%)' per group; groups under MIN_N show the count only."""
    g = df.groupby(by, observed=True)[flag].agg(["sum", "count"])
    out = []
    for idx, r in g.iterrows():
        k, n = int(r["sum"]), int(r["count"])
        txt = f"{k} of {n}" if n < MIN_N else f"{k} of {n} ({k / n:.0%})"
        out.append((idx, txt))
    return pd.DataFrame(out, columns=[by, flag]).set_index(by)


def coverage(ws: pd.DataFrame) -> pd.DataFrame:
    return ws["wn_source"].value_counts().rename("users").to_frame()


def band_sizes(ws: pd.DataFrame) -> pd.DataFrame:
    g = ws.groupby("band", observed=False)
    return pd.DataFrame(
        {
            "users": g.size(),
            "median votes needed": g["votes_needed"].median().round(0),
            "median voter_count": g["voter_count"].median().round(0),
            "Nov 3 races": g["ged"].apply(lambda s: (s == "2026-11-03").sum()),
        }
    )


def feature_table(ws: pd.DataFrame) -> pd.DataFrame:
    cols = [rate(ws, f"acted_{a}").rename(columns={f"acted_{a}": AREA_NAMES[a]}) for a in AREAS]
    t = pd.concat(cols, axis=1)
    allrow = {
        AREA_NAMES[a]: f"{int(ws[f'acted_{a}'].sum())} of {len(ws)} ({ws[f'acted_{a}'].mean():.0%})"
        for a in AREAS
    }
    t.loc["all"] = allrow
    return t


def feature_depth(ws: pd.DataFrame) -> pd.DataFrame:
    """Median action-days among users who acted in the area."""
    rows = {}
    for b, d in ws.groupby("band", observed=True):
        rows[b] = {
            AREA_NAMES[
                a
            ]: f"{d.loc[d[f'acted_{a}'], f'days_{a}'].median():.0f} (n={int(d[f'acted_{a}'].sum())})"
            if d[f"acted_{a}"].sum() >= MIN_N
            else f"n={int(d[f'acted_{a}'].sum())}"
            for a in AREAS
        }
    return pd.DataFrame(rows).T


def channel_table(ws: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in CHANNELS:
        ws[f"used_{c}"] = ws[f"sends_{c}"] > 0
        cols.append(rate(ws, f"used_{c}").rename(columns={f"used_{c}": c}))
    t = pd.concat(cols, axis=1)
    t["unattributed share of sends"] = ws.groupby("band", observed=True).apply(
        lambda d: f"{d['sends_unattributed'].sum()} of {d['sends_total'].sum()}", include_groups=False
    )
    return t


def volume_table(ws: pd.DataFrame) -> pd.DataFrame:
    def summ(d: pd.DataFrame) -> pd.Series:
        t = d[d["texts_known"]]
        p = d[d["paid"]]
        return pd.Series(
            {
                "users": len(d),
                "texted": f"{int(d['texted'].sum())} of {len(d)}",
                "texted, count unknown": f"{int(d['texts_unknown'].sum())}",
                "median texts (texters)": f"{t['texts_sent'].median():,.0f}" if len(t) else "-",
                "median texts per vote needed (texters)": f"{t['texts_per_vote'].median():.2f}"
                if len(t)
                else "-",
                f"over {BUNDLE_TEXTS:,} texts": f"{int(d['over_bundle'].sum())} of {len(d)}",
                "paid for outreach": f"{len(p)} of {len(d)}",
                "median outreach $ (payers)": f"${p['outreach_usd'].median():,.0f}" if len(p) else "-",
                "total outreach $": f"${d['outreach_usd'].sum():,.0f}",
            }
        )

    t = ws.groupby("band", observed=True).apply(summ, include_groups=False)
    t.loc["all"] = summ(ws)
    return t


def allowance_table(ws: pd.DataFrame, qs: tuple[float, ...] = (0.5, 0.75, 0.9)) -> pd.DataFrame:
    """Texts that cover 50/75/90% of each band: all Pro (non-texters at 0) and texters only.

    Texters with an unknown count are left out of both, so the cover points lean low where
    unknowns are many (small and early races).
    """
    rows = {}
    for b, d in ws.groupby("band", observed=True):
        if b == "unknown":
            continue
        allp = d.loc[~d["texts_unknown"], "texts_sent"]
        tex = d.loc[d["texts_known"], "texts_sent"]
        r = {"users (known count)": f"{len(allp)} of {len(d)}"}
        for q in qs:
            r[f"all Pro {q:.0%}"] = f"{allp.quantile(q):,.0f}"
        r["texters (known count)"] = f"{len(tex)}"
        for q in qs:
            r[f"texters {q:.0%}"] = f"{tex.quantile(q):,.0f}" if len(tex) >= MIN_N else "-"
        rows[b] = r
    return pd.DataFrame(rows).T


def within(ws: pd.DataFrame, flag: str, by: str) -> pd.DataFrame:
    """A band difference must hold inside each level of `by` (composition check)."""
    g = ws.groupby([by, "band"], observed=True)[flag].agg(["sum", "count"])
    g["cell"] = [
        f"{int(s)}/{int(n)}" + (f" ({s / n:.0%})" if n >= MIN_N else "")
        for s, n in zip(g["sum"], g["count"], strict=False)
    ]
    return g["cell"].unstack("band")


def sanity(ws: pd.DataFrame) -> str:
    over = (ws["votes_needed"] > ws["voter_count"]).sum()
    return f"votes needed above the district's voter_count: {int(over)} of {int(ws['voter_count'].notna().sum())}"


def main() -> None:
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 30)
    ws = load()
    print(f"Run {ws['run_ts'].iloc[0]} UTC. Pro users with a 2026 election: {len(ws):,}")
    print(ws["pro_status"].value_counts().to_string())
    print("\n== votes needed source ==\n", coverage(ws))
    print(sanity(ws))
    print("\n== bands ==\n", band_sizes(ws))
    fm = ws[ws["feature_measured"]]
    print("\n== acted in area, races from Jul 15 (feature events measurable) ==\n", feature_table(fm))
    print("\n== median action-days among those who acted ==\n", feature_depth(fm))
    print("\n== texts that cover 50/75/90% of each band ==\n", allowance_table(ws))
    print("\n== channel use ==\n", channel_table(ws))
    print("\n== texting volume and spend ==\n", volume_table(ws))
    for flag in ["acted_opponents", "acted_ai"]:
        for by in ["signup_era", "office_level"]:
            print(f"\n== {flag} within {by} (races from Jul 15) ==\n", within(fm, flag, by))
    for flag in ["over_bundle", "over_bundle_upper", "paid"]:
        for by in ["signup_era", "office_level", "pro_status"]:
            print(f"\n== {flag} within {by} ==\n", within(ws, flag, by))
    for name, mask in [
        ("without races before Jul 15", ~ws["early_election"]),
        ("without future races", ~ws["future_election"]),
    ]:
        print(f"\n== robustness: {name} ==\n", volume_table(ws[mask]))
    print("\n== features without future races ==\n", feature_table(fm[~fm["future_election"]]))


if __name__ == "__main__":
    main()
