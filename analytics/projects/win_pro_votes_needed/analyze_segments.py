"""Which purchase-time segments separate Win Pro usage, against race size as the yardstick.

Spec: win_pro_pricing_segments_brief.yaml. Pure pandas over datasets/ (run build_segments.py
first). Prints every table and writes segments_results.json.

Run from analytics/:
    uv run --with pyarrow --with scipy python projects/win_pro_votes_needed/analyze_segments.py
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np
import pandas as pd
from analyze import AREA_NAMES, AREAS, BUNDLE_TEXTS, DATA, HERE, MIN_N, load
from scipy import stats

ERA_MIN = 15  # each era needs this many in the group for the direction check
Q_CLEAR = 0.10
BOOT = 2000
TEXT_CENTS = (3.1, 3.5)
RNG = np.random.default_rng(2810)


@dataclass(frozen=True)
class Outcome:
    key: str
    label: str
    kind: str  # "rate" or "median"
    col: str
    subset: str | None = None  # boolean column restricting who is measured


OUTCOMES = [
    Outcome("texted", "Texted", "rate", "texted"),
    # Among texters: an allowance only matters to people who text (DATA-2810 amendment 3).
    Outcome(
        "over_5000", f"Over {BUNDLE_TEXTS:,} texts (texters, lower bound)", "rate", "over_bundle", "texted"
    ),
    Outcome("median_texts", "Median texts (texters, known count)", "median", "texts_sent", "texts_known"),
    Outcome("paid", "Paid for outreach", "rate", "paid"),
    Outcome("median_usd", "Median net outreach $ (payers)", "median", "outreach_usd", "paid"),
    Outcome("no_outreach", "No outreach at all", "rate", "no_outreach"),
    Outcome("robocall", "Used robocall", "rate", "used_robocall"),
    Outcome("social", "Used social", "rate", "used_social"),
    Outcome("door_knocking", "Used door knocking", "rate", "used_door_knocking"),
    Outcome("phone_banking", "Used phone banking", "rate", "used_phone_banking"),
] + [Outcome(f"area_{a}", f"Acted: {AREA_NAMES[a]}", "rate", f"acted_{a}", "feature_measured") for a in AREAS]


def _bucket(s: pd.Series, keep: list[str] | None = None, other: str = "other") -> pd.Series:
    s = s.fillna("unknown").astype(str)
    if keep is not None:
        s = s.where(s.isin([*keep, "unknown"]), other)
    return s


def dimensions(ws: pd.DataFrame) -> dict[str, pd.Series]:
    """Purchase-time dimensions as labeled Series. Values under MIN_N fold into 'other'."""
    d: dict[str, pd.Series] = {}
    lvl = ws["office_level_norm"].replace({"local": "city"})
    d["Office level"] = _bucket(lvl)
    cats = ws["office_category"].value_counts()
    d["Office category"] = _bucket(ws["office_category"], cats[cats >= MIN_N].index.tolist())
    # State is reported as background only (DATA-2810 amendment 5): nobody would price by state, and
    # its gaps tracked single channels rather than customer types.
    d["Incumbent"] = _bucket(ws["is_incumbent"].map({True: "incumbent", False: "challenger"}))
    d["Repeat candidate"] = _bucket(
        ws["election_year_count"].map(
            lambda x: np.nan if pd.isna(x) else ("repeat" if x > 1 else "first-time")
        )
    )
    d["Partisan"] = _bucket(
        ws["partisan_type"].replace({"partisan for primary only": "partisan"}),
    )
    d["Contested"] = _bucket(ws["is_uncontested"].map({True: "uncontested", False: "contested"}))
    opp = ws["number_of_opponents"]
    d["Opponents"] = _bucket(
        pd.Series(
            np.select([opp <= 1, opp == 2, opp >= 3], ["0 to 1", "2", "3+"], ""), index=ws.index
        ).replace("", np.nan)
    )
    seats = ws["seats_available"]
    d["Seats"] = _bucket(
        pd.Series(
            np.select([seats == 1, seats > 1], ["single seat", "multi-seat"], ""), index=ws.index
        ).replace("", np.nan)
    )
    lead = (ws["ged"] - ws["first_pay_date"]).dt.days
    d["Pro lead time"] = _bucket(
        pd.Series(
            np.select(
                [lead < 45, lead < 90, lead < 180, lead >= 180],
                ["under 45 days", "45-89 days", "90-179 days", "180+ days"],
                "",
            ),
            index=ws.index,
        ).replace("", np.nan)
    )
    o2p = (ws["first_pay_date"] - ws["onboarded_date"]).dt.days
    d["Onboarding to Pro"] = _bucket(
        pd.Series(
            np.select(
                [o2p <= 0, o2p <= 7, o2p <= 30, o2p > 30],
                ["same day or before", "1-7 days", "8-30 days", "31+ days"],
                "",
            ),
            index=ws.index,
        ).replace("", np.nan)
    )
    d["ICP office"] = _bucket(ws["is_win_icp"].map({True: "ICP", False: "not ICP"}))
    d["Sales vs marketing"] = _bucket(ws["acquisition_motion"])
    src = ws["hs_analytics_source"].map(
        {
            "OFFLINE": "sales (offline)",
            "DIRECT_TRAFFIC": "direct",
            "PAID_SEARCH": "paid search",
            "ORGANIC_SEARCH": "organic search",
            "PAID_SOCIAL": "paid social",
        }
    )
    d["Acquisition source"] = _bucket(src.where(src.notna() | ws["hs_analytics_source"].isna(), "other"))
    return d


def _day(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, utc=True)
    return s.dt.tz_localize(None).dt.normalize()


def prep() -> pd.DataFrame:
    ws = load()
    seg = pd.read_parquet(DATA / "segments.parquet").drop(columns="run_ts")
    ws = ws.merge(seg, on="user_id", how="left", validate="one_to_one")
    cd = pd.read_parquet(DATA / "cas_dates.parquet")
    assert (cd["run_ts"] == ws["run_ts"].iloc[0]).all(), "cas_dates.parquet is from another run"
    ws = ws.merge(
        cd[["user_id", "cas_qualified_at", "first_text_at"]], on="user_id", how="left", validate="one_to_one"
    )
    ws["first_pay_date"] = _day(ws["first_subscription_payment_at"])
    ws["onboarded_date"] = _day(ws["onboarding_completed_at"])
    ws["ged"] = _day(ws["ged"])
    for c in ["texting", "robocall", "social", "door_knocking", "phone_banking"]:
        ws[f"used_{c}"] = ws[f"sends_{c}"] > 0
    ws["texts_sent"] = pd.to_numeric(ws["texts_sent"], errors="coerce").astype(float)
    ws["cas_support"] = cas_support(ws)
    return ws


def cas_support(ws: pd.DataFrame) -> pd.Series:
    """CaS contact by the results-coverage rule (1+ call, 2+ emails or texts, or a meeting), split by
    whether contact met that rule before any outreach. Every upgrade is offered a CaS call, so this
    marks customers who engaged with it, which is partly their own intent."""
    met = (ws["cas_calls"] >= 1) | (ws["cas_msgs"] >= 2) | (ws["cas_meetings"] >= 1)
    first_cas = pd.to_datetime(ws["cas_qualified_at"], utc=True)
    first_send = pd.to_datetime(ws["first_send_at"], utc=True)
    before = first_send.isna() | (first_cas < first_send)
    return pd.Series(
        np.select([~met, before], ["no CaS contact", "CaS before outreach"], "CaS after outreach began"),
        index=ws.index,
    )


# ---------- statistics ----------


def _wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    p = k / n
    d = 1 + z**2 / n
    c = (p + z**2 / (2 * n)) / d
    h = z * np.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / d
    return c - h, c + h


def rate_gap(a: pd.Series, b: pd.Series) -> dict:
    k1, n1, k2, n2 = int(a.sum()), len(a), int(b.sum()), len(b)
    p1, p2 = k1 / n1, k2 / n2
    l1, u1 = _wilson(k1, n1)
    l2, u2 = _wilson(k2, n2)
    # Newcombe hybrid score interval for p1 - p2.
    lo = (p1 - p2) - np.sqrt((p1 - l1) ** 2 + (u2 - p2) ** 2)
    hi = (p1 - p2) + np.sqrt((u1 - p1) ** 2 + (p2 - l2) ** 2)
    table = [[k1, n1 - k1], [k2, n2 - k2]]
    p = (
        stats.fisher_exact(table).pvalue
        if min(k1, k2, n1 - k1, n2 - k2) < 5
        else stats.chi2_contingency(table)[1]
    )
    return {
        "group": p1,
        "rest": p2,
        "gap": p1 - p2,
        "lo": lo,
        "hi": hi,
        "p": p,
        "k": k1,
        "n": n1,
        "n_rest": n2,
    }


def median_gap(a: pd.Series, b: pd.Series) -> dict:
    a, b = a.to_numpy(float), b.to_numpy(float)
    m1, m2 = np.median(a), np.median(b)
    boots = np.median(RNG.choice(a, (BOOT, len(a))), axis=1) / np.median(
        RNG.choice(b, (BOOT, len(b))), axis=1
    )
    p = stats.mannwhitneyu(a, b).pvalue
    return {
        "group": m1,
        "rest": m2,
        "gap": m1 / m2,
        "lo": float(np.quantile(boots, 0.025)),
        "hi": float(np.quantile(boots, 0.975)),
        "p": p,
        "n": len(a),
        "n_rest": len(b),
    }


def gap(o: Outcome, df: pd.DataFrame, in_group: pd.Series) -> dict | None:
    if o.subset:
        keep = df[o.subset].astype(bool)
        df, in_group = df[keep], in_group[keep]
    a, b = df.loc[in_group, o.col], df.loc[~in_group, o.col]
    if len(a) < MIN_N or len(b) < MIN_N:
        return None
    return rate_gap(a.astype(bool), b.astype(bool)) if o.kind == "rate" else median_gap(a, b)


def _signed(o: Outcome, g: dict) -> float:
    """Direction on a common scale: points for rates, log ratio for medians."""
    return g["gap"] if o.kind == "rate" else float(np.log(g["gap"]))


def era_direction(o: Outcome, df: pd.DataFrame, in_group: pd.Series, raw: float) -> str:
    signs = []
    for _, sub in df.groupby("signup_era"):
        g_in = in_group.loc[sub.index]
        s = sub[sub[o.subset].astype(bool)] if o.subset else sub
        gi = g_in.loc[s.index]
        a, b = s.loc[gi, o.col], s.loc[~gi, o.col]
        if len(a) < ERA_MIN or len(b) < ERA_MIN:
            return "not checkable"
        e = (
            a.astype(bool).mean() - b.astype(bool).mean()
            if o.kind == "rate"
            else np.log(a.median() / b.median())
        )
        signs.append(np.sign(e))
    return "same in both eras" if all(s == np.sign(raw) and s != 0 for s in signs) else "differs by era"


def stratified_share(o: Outcome, df: pd.DataFrame, in_group: pd.Series, raw: float, by: str) -> float:
    """Gap averaged within the levels of `by` (weighted by group size), as a share of the raw gap."""
    s = df[df[o.subset].astype(bool)] if o.subset else df
    gi = in_group.loc[s.index]
    num = den = 0.0
    for _, band in s.groupby(by, observed=True):
        bi = gi.loc[band.index]
        a, b = band.loc[bi, o.col], band.loc[~bi, o.col]
        if len(a) < 1 or len(b) < 1 or (o.kind == "median" and (len(a) < 5 or len(b) < 5)):
            continue
        e = (
            a.astype(bool).mean() - b.astype(bool).mean()
            if o.kind == "rate"
            else np.log(a.median() / b.median())
        )
        num += len(a) * e
        den += len(a)
    return (num / den) / raw if den and raw else np.nan


def self_serve_share(o: Outcome, df: pd.DataFrame, in_group: pd.Series, raw: float) -> float:
    """The gap among customers with no CaS contact, as a share of the raw gap. CaS engagement
    interacts with segments rather than confounding them (gaps appear only among the engaged), so
    an average across CaS groups would hide it; the self-serve group is read on its own."""
    sub = df[df["cas_support"] == "no CaS contact"]
    g = gap(o, sub, in_group.loc[sub.index])
    return _signed(o, g) / raw if g is not None and raw else np.nan


def bh(p: pd.Series) -> pd.Series:
    order = p.sort_values()
    m = len(order)
    q = (order * m / np.arange(1, m + 1))[::-1].cummin()[::-1].clip(upper=1)
    return q.reindex(p.index)


def grid(ws: pd.DataFrame, dims: dict[str, pd.Series]) -> pd.DataFrame:
    rows = []
    for dname, lab in dims.items():
        for val in sorted(lab.unique()):
            in_group = lab == val
            for o in OUTCOMES:
                g = gap(o, ws, in_group)
                if g is None:
                    continue
                raw = _signed(o, g)
                rows.append(
                    {
                        "dimension": dname,
                        "value": val,
                        "outcome": o.key,
                        "label": o.label,
                        "kind": o.kind,
                        **g,
                        "era": era_direction(o, ws, in_group, raw),
                        "within_band_share": stratified_share(o, ws, in_group, raw, "band")
                        if dname != "Votes needed"
                        else np.nan,
                        "self_serve_share": self_serve_share(o, ws, in_group, raw)
                        if dname not in ("Votes needed", "CaS support")
                        else np.nan,
                    }
                )
    return pd.DataFrame(rows)


def classify(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    g["q"] = np.nan
    # The yardstick and CaS support are corrected as their own families, apart from the segments.
    fam_key = g["dimension"].where(g["dimension"].isin(["Votes needed", "CaS support"]), "segments")
    for _fam, idx in g.groupby(fam_key).groups.items():
        g.loc[idx, "q"] = bh(g.loc[idx, "p"])
    same = g["era"] == "same in both eras"
    # A significant gap needs the same direction in both signup eras to count as Clear. One that flips,
    # or that cannot be checked because an era is too small, is kept apart rather than called no difference.
    sig = g["q"] < Q_CLEAR
    g["bin"] = np.select(
        [sig & same, sig & (g["era"] == "differs by era"), sig],
        ["Clear", "Differs by era", "Era not checkable"],
        "No difference",
    )
    w = g["within_band_share"]
    g["race_size"] = np.where(w < 0.5, "explained by race size", "")
    c = g["self_serve_share"]
    g["cas"] = np.where(c < 0.5, "only among CaS-engaged", np.where(c.isna(), "self-serve too few", ""))
    return g


# ---------- context tables ----------


def cramers_v(a: pd.Series, b: pd.Series) -> float:
    t = pd.crosstab(a, b)
    if t.shape[0] < 2 or t.shape[1] < 2:
        return np.nan
    chi2 = stats.chi2_contingency(t, correction=False)[0]
    return float(np.sqrt(chi2 / (t.to_numpy().sum() * (min(t.shape) - 1))))


def overlap(dims: dict[str, pd.Series]) -> pd.DataFrame:
    names = list(dims)
    return pd.DataFrame(
        [[cramers_v(dims[a], dims[b]) if a != b else np.nan for b in names] for a in names],
        index=names,
        columns=names,
    ).round(2)


def detectable_gap(n: int, total: int, p: float = 0.47) -> float:
    # Two-sided 5%, 80% power, group vs the rest.
    return 2.8 * np.sqrt(p * (1 - p) * (1 / n + 1 / (total - n)))


def fmt_gap(r: pd.Series) -> str:
    if r["kind"] == "rate":
        return f"{r['group']:.0%} vs {r['rest']:.0%} ({r['gap'] * 100:+.0f} pts, {r['lo'] * 100:+.0f} to {r['hi'] * 100:+.0f})"
    unit = "$" if "usd" in r["outcome"] else ""
    return (
        f"{unit}{r['group']:,.0f} vs {unit}{r['rest']:,.0f} (x{r['gap']:.2f}, {r['lo']:.2f} to {r['hi']:.2f})"
    )


def pricing_units(r: pd.Series) -> str:
    if r["outcome"] == "median_texts":
        d = r["group"] - r["rest"]
        return f"{d:+,.0f} texts = ${d * TEXT_CENTS[0] / 100:+,.0f} to ${d * TEXT_CENTS[1] / 100:+,.0f}"
    if r["outcome"] == "median_usd":
        return f"${r['group'] - r['rest']:+,.0f} per payer"
    return ""


def main() -> None:
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 40)
    pd.set_option("display.max_rows", 400)
    pd.set_option("display.max_colwidth", 80)
    ws = prep()
    run_ts = ws["run_ts"].iloc[0]
    dims = dimensions(ws)
    out: dict = {"run_ts": str(run_ts), "n": len(ws), "n_feature": int(ws["feature_measured"].sum())}
    print(
        f"Run {run_ts} UTC. Paying Pro with a 2026 election: {len(ws):,}; feature outcomes on {out['n_feature']:,}"
    )

    print("\n== dimension values (users) ==")
    sizes = {d: s.value_counts().to_dict() for d, s in dims.items()}
    for d, v in sizes.items():
        print(f"  {d}: {v}")
    out["sizes"] = sizes

    print("\n== smallest reliably detectable gap in a ~47% rate, group vs rest ==")
    det = {n: round(detectable_gap(n, len(ws)) * 100) for n in [30, 60, 100, 200, 300, len(ws) // 2]}
    print("  " + ", ".join(f"{n} users: {v} pts" for n, v in det.items()))
    out["detectable_gap_pts"] = det

    dims_y = {**dims, "Votes needed": ws["band"].astype(str), "CaS support": ws["cas_support"]}
    ov = overlap(dims_y)
    print("\n== overlap between dimensions (Cramér's V; 0 independent, 1 same thing) ==\n", ov)
    out["overlap"] = ov.to_dict()

    g = classify(grid(ws, dims_y))
    g["shown"] = g.apply(fmt_gap, axis=1)
    g["pricing"] = g.apply(pricing_units, axis=1)

    ys = g[g["dimension"] == "Votes needed"].copy()
    ys["abs"] = np.where(ys["kind"] == "rate", ys["gap"].abs(), np.log(ys["gap"]).abs())
    yard = ys.sort_values("abs").groupby("outcome").tail(1).set_index("outcome")
    print("\n== yardstick: the largest votes-needed band gap per outcome ==")
    print(yard[["value", "shown", "bin"]])
    out["yardstick"] = (
        yard[["value", "group", "rest", "gap", "lo", "hi", "q", "bin"]].reset_index().to_dict("records")
    )

    print(
        "\n== CaS support (engaged with the CaS call offered at upgrade; post-purchase, a service not a segment) =="
    )
    print("  " + ", ".join(f"{k}: {v}" for k, v in ws["cas_support"].value_counts().items()))
    cg = g[g["dimension"] == "CaS support"]
    print(cg[["value", "label", "n", "shown", "q", "bin"]].to_string(index=False))
    out["cas_support"] = cg.drop(columns=["shown"]).replace({np.nan: None}).to_dict("records")
    icp = dims["ICP office"]
    for flag in ["texted", "paid", "over_bundle", "no_outreach"]:
        t = ws.groupby([ws["cas_support"], icp])[flag].apply(
            lambda x: f"{int(x.sum())}/{len(x)}" + (f" ({x.mean():.0%})" if len(x) >= MIN_N else "")
        )
        print(f"\n  {flag} by CaS support x ICP office\n", t.unstack().to_string())

    seg = g[~g["dimension"].isin(["Votes needed", "CaS support"])].copy()
    seg["abs"] = np.where(seg["kind"] == "rate", seg["gap"].abs(), np.log(seg["gap"]).abs())
    seg = seg.join(yard["abs"].rename("yard_abs"), on="outcome")
    seg["vs_race_size"] = (seg["abs"] / seg["yard_abs"]).round(2)
    print(f"\n== tests: {len(seg)} segment one-vs-rest comparisons ==")
    print(seg["bin"].value_counts().to_string())

    cols = [
        "dimension",
        "value",
        "label",
        "n",
        "shown",
        "q",
        "era",
        "race_size",
        "cas",
        "vs_race_size",
        "pricing",
    ]
    for b in ["Clear", "Differs by era", "Era not checkable"]:
        print(f"\n== {b} ==")
        print(seg[seg["bin"] == b].sort_values(["dimension", "value", "q"])[cols].to_string(index=False))

    print("\n== per dimension: Clear; beyond race size; and also present among self-serve customers ==")
    keep = seg[
        (seg["bin"] == "Clear") & (seg["race_size"] == "") & (seg["cas"] == "") & (seg["value"] != "unknown")
    ]
    print(
        keep[["dimension", "value", "label", "n", "shown", "self_serve_share"]]
        .round(2)
        .to_string(index=False)
    )
    summ = (
        seg.assign(
            clear=seg["bin"] == "Clear",
            clear_beyond=(seg["bin"] == "Clear") & (seg["race_size"] == ""),
            clear_self_serve=(seg["bin"] == "Clear") & (seg["race_size"] == "") & (seg["cas"] == ""),
            self_serve_too_few=(seg["bin"] == "Clear")
            & (seg["race_size"] == "")
            & (seg["cas"] == "self-serve too few"),
        )
        .groupby("dimension")[["clear", "clear_beyond", "clear_self_serve", "self_serve_too_few"]]
        .sum()
        .sort_values(["clear_self_serve", "clear_beyond", "clear"], ascending=False)
    )
    print(summ)
    out["summary"] = summ.to_dict("index")

    print("\n== robustness for Clear results: without future elections, and activated only ==")
    clear = seg[seg["bin"] == "Clear"]
    rob = []
    o_by_key = {o.key: o for o in OUTCOMES}
    for _, r in clear.iterrows():
        o = o_by_key[r["outcome"]]
        lab = dims[r["dimension"]]
        row = {"dimension": r["dimension"], "value": r["value"], "outcome": r["outcome"], "all": r["shown"]}
        for name, mask in [("no future", ~ws["future_election"]), ("activated", ws["activated"])]:
            sub = ws[mask]
            gg = gap(o, sub, (lab == r["value"]).loc[sub.index])
            row[name] = (
                "too few" if gg is None else fmt_gap(pd.Series({**gg, "kind": o.kind, "outcome": o.key}))
            )
        rob.append(row)
    rob_df = pd.DataFrame(rob)

    print("\n== Clear results inside each CaS support group ==")
    within = []
    for _, r in clear.iterrows():
        o = o_by_key[r["outcome"]]
        lab = dims[r["dimension"]]
        row = {
            "dimension": r["dimension"],
            "value": r["value"],
            "outcome": r["outcome"],
            "self-serve share": round(r["self_serve_share"], 2),
        }
        for lvl, sub in ws.groupby("cas_support"):
            gg = gap(o, sub, (lab == r["value"]).loc[sub.index])
            row[lvl] = (
                "too few" if gg is None else fmt_gap(pd.Series({**gg, "kind": o.kind, "outcome": o.key}))
            )
        within.append(row)
    print(pd.DataFrame(within).to_string(index=False) if within else "  none")
    print(rob_df.to_string(index=False) if len(rob_df) else "  none")

    print("\n== unattributed share of sends per Clear segment value ==")
    for (d, v), _ in clear.groupby(["dimension", "value"]):
        m = dims[d] == v
        print(
            f"  {d} = {v}: {int(ws.loc[m, 'sends_unattributed'].sum())} of {int(ws.loc[m, 'sends_total'].sum())} sends"
            f" (rest {int(ws.loc[~m, 'sends_unattributed'].sum())} of {int(ws.loc[~m, 'sends_total'].sum())})"
        )

    team = ws.assign(team=ws["team_invites"] > 0).groupby("pro_status")["team"].agg(["sum", "count"])
    print("\n== team use (invited anyone; launched 2026-09-10) ==\n", team)
    out["team"] = team.to_dict("index")

    out["grid"] = g.drop(columns=["shown"]).replace({np.nan: None}).to_dict("records")
    (HERE / "segments_results.json").write_text(json.dumps(out, indent=1, default=str))
    print("\nwrote segments_results.json")


if __name__ == "__main__":
    main()
