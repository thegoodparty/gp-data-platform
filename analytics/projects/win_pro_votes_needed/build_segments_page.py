"""Render the pricing-segments findings page from the same analyze_segments.py functions.

Run from analytics/ after build_segments.py:
    uv run --with pyarrow --with scipy --with pyyaml python projects/win_pro_votes_needed/build_segments_page.py <out.html>

Layout follows .claude/skills/analytics-process/references/findings-page.md. Every number is written
from a variable. The explorer embeds aggregate counts only (no per-user rows).
"""

from __future__ import annotations

import html
import inspect
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

PROJ = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJ))
import analyze_segments as S  # noqa: E402
import build_segments as BS  # noqa: E402

esc = html.escape
SEP = '<span class="sep">|</span>'
FADE_N = 100

ws = S.prep()
RUN = ws["run_ts"].iloc[0]
N = len(ws)
dims = S.dimensions(ws)
dims_y = {**dims, "Votes needed": ws["band"].astype(str), "CaS support": ws["cas_support"]}
g = S.classify(S.grid(ws, dims_y))
seg = g[~g["dimension"].isin(["Votes needed", "CaS support"])].copy()
O = {o.key: o for o in S.OUTCOMES}  # noqa: E741
RATE_OUT = [o for o in S.OUTCOMES if o.kind == "rate"]
OUTREACH_KEYS = [
    "texted",
    "over_5000",
    "paid",
    "no_outreach",
    "robocall",
    "social",
    "door_knocking",
    "phone_banking",
]
FEATURE_KEYS = [f"area_{a}" for a in S.AREAS]
SHORT = {
    "texted": "Texted",
    "over_5000": "Over 5,000 texts, of texters",
    "paid": "Paid for outreach",
    "no_outreach": "No outreach at all",
    "robocall": "Robocall",
    "social": "Social",
    "door_knocking": "Door knocking",
    "phone_banking": "Phone banking",
    "median_texts": "Median texts per texter",
    "median_usd": "Median outreach $ per payer",
    **{f"area_{a}": S.AREA_NAMES[a] for a in S.AREAS},
}
CAS_ORDER = ["no CaS contact", "CaS before outreach", "CaS after outreach began"]
CAS_LABEL = {
    "no CaS contact": "No CaS contact",
    "CaS before outreach": "CaS before any outreach",
    "CaS after outreach began": "CaS only after outreach began",
}
STRATA = [("all", "All Pro customers")] + [(c, CAS_LABEL[c]) for c in CAS_ORDER]


def sub_for(o, d):
    return d[d[o.subset].astype(bool)] if o.subset else d


def pct(x):
    return f"{x:.0%}"


# ---------- explorer data (aggregates only) ----------
def cell(o, d):
    s = sub_for(o, d)
    return [int(s[o.col].astype(bool).sum()), len(s)]


def med_cell(o, d):
    s = sub_for(o, d)[o.col].astype(float)
    return [None if len(s) == 0 else float(s.median()), len(s)]


explorer = {
    "strata": [k for k, _ in STRATA],
    "strataLabel": dict(STRATA),
    "outcomes": {},
    "dims": {},
    "ref": {},
}
for o in S.OUTCOMES:
    explorer["outcomes"][o.key] = {"label": SHORT.get(o.key, o.label), "kind": o.kind}
for sk, _ in STRATA:
    d = ws if sk == "all" else ws[ws["cas_support"] == sk]
    explorer["ref"][sk] = {o.key: (cell(o, d) if o.kind == "rate" else med_cell(o, d)) for o in S.OUTCOMES}
for dname, lab in {**dims, "Votes needed": ws["band"].astype(str)}.items():
    vals = list(lab.value_counts().index)
    if dname == "Votes needed":
        vals = [b for b in S.BANDS if b in vals] if hasattr(S, "BANDS") else vals
    entry = {"values": vals, "cells": {}, "bins": {}}
    for sk, _ in STRATA:
        mask = pd.Series(True, index=ws.index) if sk == "all" else ws["cas_support"] == sk
        entry["cells"][sk] = {
            v: {
                o.key: (
                    cell(o, ws[mask & (lab == v)]) if o.kind == "rate" else med_cell(o, ws[mask & (lab == v)])
                )
                for o in S.OUTCOMES
            }
            for v in vals
        }
    gd = g[g["dimension"] == dname]
    for _, r in gd.iterrows():
        tags = [t for t in [r.get("race_size", ""), r.get("cas", "")] if isinstance(t, str) and t]
        entry["bins"].setdefault(r["value"], {})[r["outcome"]] = {
            "bin": r["bin"],
            "q": float(r["q"]),
            "tags": tags,
        }
    explorer["dims"][dname] = entry
from analyze import BANDS, over_range, why_range  # noqa: E402
from figures import CSS as FIG_CSS  # noqa: E402
from figures import volume_chart  # noqa: E402

explorer["dims"]["Votes needed"]["values"] = [
    b for b in BANDS if b in explorer["dims"]["Votes needed"]["values"]
]
DIM_ORDER = [*dims, "Votes needed"]

# ---------- headline numbers ----------
cas_n = ws["cas_support"].value_counts()
by_cas = {c: ws[ws["cas_support"] == c] for c in CAS_ORDER}
nocas, pre, post = by_cas["no CaS contact"], by_cas["CaS before outreach"], by_cas["CaS after outreach began"]
engaged = ws[ws["cas_support"] != "no CaS contact"]
icp = dims["ICP office"]


def rate_of(d, col):
    return d[col].astype(bool).mean()


def kn(d, col):
    return f"{int(d[col].astype(bool).sum())} of {len(d)}"


icp_pre = (
    rate_of(pre[icp[pre.index] == "ICP"], "texted"),
    rate_of(pre[icp[pre.index] == "not ICP"], "texted"),
)
icp_no = (
    rate_of(nocas[icp[nocas.index] == "ICP"], "texted"),
    rate_of(nocas[icp[nocas.index] == "not ICP"], "texted"),
)
icp_all = (rate_of(ws[icp == "ICP"], "texted"), rate_of(ws[icp == "not ICP"], "texted"))
kt = ws[ws["texts_known"]]
med_texts_no = kt[kt["cas_support"] == "no CaS contact"]["texts_sent"].median()
# The no-CaS volume gap after Amplitude filled counts (DATA-2810 amendment 4): its interval and within the 2,500+ bands.
_tx_gap = S.gap(O["median_texts"], ws, ws["cas_support"] == "no CaS contact")
_big_kt = kt[kt["band"].isin(["2,500-9,999", "10,000+"])]
med_texts_eng = kt[kt["cas_support"] != "no CaS contact"]["texts_sent"].median()
paid = ws[ws["paid"]]
med_usd_no = paid[paid["cas_support"] == "no CaS contact"]["outreach_usd"].median()
med_usd_eng = paid[paid["cas_support"] != "no CaS contact"]["outreach_usd"].median()
band_med = kt.groupby("band", observed=True)["texts_sent"].median()
texters = ws[ws["texted"]]
big = texters[texters["band"].isin(["2,500-9,999", "10,000+"])]


pct_range = over_range


# Order of CaS contact (dated when it met the rule) vs first text, among texters with CaS contact.
fc = pd.to_datetime(ws["cas_qualified_at"], utc=True)
fs = pd.to_datetime(ws["first_text_at"], utc=True)
tx = ws[ws["texted"] & (ws["cas_support"] != "no CaS contact") & fc.notna() & fs.notna()]
cas_first = int((fc[tx.index] < fs[tx.index]).sum())
lead_days = (fs[tx.index] - fc[tx.index]).dt.days
lead_med = float(lead_days[lead_days > 0].median())

counts = seg["bin"].value_counts()
n_tests = len(seg)
n_canceled = int((ws["pro_status"] == "canceled after election").sum())
n_uncontested = int((ws["is_uncontested"] == True).sum())  # noqa: E712
n_sales = int((ws["acquisition_motion"] == "sales_sourced").sum())
assert n_sales == int((ws["hs_analytics_source"] == "OFFLINE").sum()), "offline is no longer exactly sales"
clear = seg[seg["bin"] == "Clear"]
# Unknown values mark missing civics data and Pro lead time is timing, not a group anyone buys as;
# both stay in the full Clear list but leave the summary tables (explained in the A4 note).
NOT_SEGMENT = (seg["value"] == "unknown") | (seg["dimension"] == "Pro lead time")
survive = clear[(clear["race_size"] == "") & (clear["cas"] == "") & ~NOT_SEGMENT.loc[clear.index]]
survive_out = survive[survive["outcome"].isin(OUTREACH_KEYS)]
survive_feat = survive[survive["outcome"].isin(FEATURE_KEYS)]
icp_clear = int((clear["dimension"] == "ICP office").sum())
icp_left = int(
    ((clear["dimension"] == "ICP office") & (clear["race_size"] == "") & (clear["cas"] == "")).sum()
)
nodiff = [
    d
    for d in dims
    if int((clear["dimension"] == d).sum() - (clear[(clear["dimension"] == d)]["value"] == "unknown").sum())
    == 0
]
det = {n: round(S.detectable_gap(n, N) * 100) for n in [30, 60, 100, 200, 300, N // 2]}
team_k = int((ws["team_invites"] > 0).sum())
team_n = int((ws["pro_status"] == "Pro today").sum())


def gaprow(dname, val, okey, d=ws):
    lab = dims[dname] if dname in dims else ws["band"].astype(str)
    o = O[okey]
    s = sub_for(o, d)
    gi = lab.loc[s.index] == val
    a, b = s.loc[gi, o.col].astype(bool), s.loc[~gi, o.col].astype(bool)
    return a.mean(), b.mean(), len(a)


def feat_pt(dname, val, okey):
    a, b, _ = gaprow(dname, val, okey)
    return f"{a:.0%} vs {b:.0%}"


# ---------- static figures ----------
def dotrow(label, k, n, ref, extra=""):
    refline = f'<i class="ref" style="left:{ref * 100:.1f}%"></i>'
    if n < S.MIN_N:
        return (
            f'<div class="lab">{esc(label)}</div><div class="track" title="{esc(label)}: {k} of {n}, under 30 people">'
            f'{refline}</div><div class="val small">{k} of {n}</div>'
        )
    p = k / n
    faded = n < FADE_N
    tip = f"{label}: {p:.0%} | {k} of {n}" + (" | under 100 people" if faded else "") + extra
    return (
        f'<div class="lab">{esc(label)}</div><div class="track" title="{esc(tip)}">{refline}'
        f'<i class="dot{" faded" if faded else ""}" style="left:{p * 100:.1f}%"></i></div>'
        f'<div class="val{" faded-t" if faded else ""}" title="{esc(tip)}">{p:.0%}</div>'
    )


def cas_panel(okey):
    o = O[okey]
    base = sub_for(o, ws)
    ref = base[o.col].astype(bool).mean()
    rows = "".join(dotrow(CAS_LABEL[c], *cell(o, by_cas[c]), ref) for c in CAS_ORDER)
    return (
        f'<div class="panel"><div class="ptitle">{esc(SHORT[okey])}</div>'
        f'<div class="pref">everyone {ref:.0%} {SEP} {int(base[o.col].astype(bool).sum())} of {len(base)}</div>'
        f'<div class="rows wide-lab">{rows}</div></div>'
    )


AXIS = "Each track runs 0% to 100%. Dashed line: everyone. Faded: under 100 people. Under 30: count only. Hover for counts."
cas_fig = (
    '<figure class="dots"><div class="scroll"><div class="panels" style="--cols:4">'
    + "".join(cas_panel(k) for k in ["texted", "paid", "over_5000", "no_outreach"])
    + f'</div></div><div class="axis-note">{AXIS} Over 5,000 counts known text counts only: the floor of the range in A2.</div></figure>'
)
cas_feat_fig = (
    '<figure class="dots"><div class="scroll"><div class="panels" style="--cols:5">'
    + "".join(cas_panel(k) for k in FEATURE_KEYS)
    + f'</div></div><div class="axis-note">Races from July 15 only, when feature tracking works. {AXIS}</div></figure>'
)


def table(headers, rows, num_from=1):
    th = "".join(f'<th class="{"num" if i >= num_from else ""}">{esc(h)}</th>' for i, h in enumerate(headers))
    body = "".join(
        "<tr>"
        + "".join(f'<td class="{"num" if i >= num_from else ""}">{c}</td>' for i, c in enumerate(r))
        + "</tr>"
        for r in rows
    )
    return f'<div class="scroll"><table><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table></div>'


def medtxt(d):
    t = d[d["texts_known"]]["texts_sent"]
    return f"{t.median():,.0f} {SEP} {len(t)}" if len(t) >= S.MIN_N else f"n={len(t)}"


def over5k(d):
    """Share of texters over the allowance: known counts as the floor, unknown-count texters as over for the ceiling."""
    t = d[d["texted"]]
    if len(t) < S.MIN_N:
        return f"n={len(t)}"
    return f"{over_range(t)} {SEP} {len(t)} texters"


def medusd(d):
    t = d[d["paid"]]["outreach_usd"]
    return f"${t.median():,.0f} {SEP} {len(t)}" if len(t) >= S.MIN_N else f"n={len(t)}"


vol_tbl = table(
    [
        "CaS group",
        "Customers",
        "Median texts per texter | texters",
        "Texters over 5,000 | texters",
        "Median outreach $ per payer | payers",
    ],
    [
        [esc(CAS_LABEL[c]), f"{len(by_cas[c])}", medtxt(by_cas[c]), over5k(by_cas[c]), medusd(by_cas[c])]
        for c in CAS_ORDER
    ]
    + [["Everyone", f"{N}", medtxt(ws), over5k(ws), medusd(ws)]],
)
band_tbl = table(
    [
        "Votes needed",
        "Customers",
        "Median texts per texter | texters",
        "Texters over 5,000 | texters",
        "Median outreach $ per payer | payers",
    ],
    [
        [
            esc(b),
            f"{int((ws['band'] == b).sum())}",
            medtxt(ws[ws["band"] == b]),
            over5k(ws[ws["band"] == b]),
            medusd(ws[ws["band"] == b]),
        ]
        for b in BANDS
        if b != "unknown"
    ]
    + [["Everyone", f"{N}", medtxt(ws), over5k(ws), medusd(ws)]],
)

icp_rows = []
for c in CAS_ORDER:
    d = by_cas[c]
    li = icp[d.index]
    icp_rows.append(
        [esc(CAS_LABEL[c])]
        + [
            (
                lambda s: f"{s.mean():.0%} {SEP} {int(s.sum())} of {len(s)}"
                if len(s) >= S.MIN_N
                else f"{int(s.sum())} of {len(s)}"
            )(d.loc[li == v, "texted"])
            for v in ["ICP", "not ICP"]
        ]
    )
icp_tbl = table(["CaS group", "ICP office: texted", "Not ICP: texted"], icp_rows)

# ---------- headline figures (CPO view: one chart per finding) ----------
vol_hero = volume_chart(ws)

# Whether people do outreach: CaS groups against the largest segment gaps that hold for self-serve customers.
# Pro lead time is timing, not a group anyone buys as; office category repeats office level; "contested"
# is compared against a rest that is mostly races with no contested data, so it measures data coverage.
NOOUT_SEGS = survive_out[
    (survive_out["outcome"] == "no_outreach")
    & ~survive_out["dimension"].isin(["Pro lead time", "Office category", "Contested"])
]
SEG_NAMES = {
    ("Office level", "city"): "City-level seats: council, school board, mayor",
    ("Office level", "state"): "State-level seats: legislature, statewide, judges",
    ("Seats", "multi-seat"): "Races electing more than one seat",
}
did_out = ~ws["no_outreach"].astype(bool)
out_ref = did_out.mean()


def out_row(label, m):
    return dotrow(label, int(did_out[m].sum()), int(m.sum()), out_ref)


# "CaS only after outreach began" did outreach by definition, so the chart compares the two groups where it is open.
out_hero = (
    '<figure class="dots"><h3 class="ctitle">Share of paying Pro customers who did any outreach</h3>'
    '<p class="axis-note">Each dot is one group. The dashed line is everyone (' + f"{out_ref:.0%}" + "). "
    "Further right means more of that group did outreach on any channel: texts, calls, door knocking, robocalls or social.</p>"
    '<div class="scroll"><div class="hero-dots">'
    '<div class="ptitle">By whether CaS reached them</div><div class="rows wide-lab">'
    + out_row("No CaS contact", ws["cas_support"] == "no CaS contact")
    + out_row("CaS before outreach", ws["cas_support"] == "CaS before outreach")
    + '</div><div class="ptitle">By customer segment: the biggest differences found</div><div class="rows wide-lab">'
    + "".join(
        out_row(
            SEG_NAMES.get((r["dimension"], r["value"]), f'{r["dimension"]}: {r["value"]}'),
            dims[r["dimension"]] == r["value"],
        )
        for _, r in NOOUT_SEGS.iterrows()
    )
    + f'</div></div></div><div class="axis-note">All {N} customers. Not shown: the {len(post)} CaS reached only after their outreach began, who did outreach by definition. Each track runs 0% to 100%. Faded: under 100 people. Hover for counts.</div></figure>'
)
# Both gaps as the chart shows them: rounded shares, segments measured against the everyone line.
seg_gap_max = max(
    abs(round(did_out[dims[r["dimension"]] == r["value"]].mean() * 100) - round(out_ref * 100))
    for _, r in NOOUT_SEGS.iterrows()
)
cas_gap = round((1 - rate_of(pre, "no_outreach")) * 100) - round((1 - rate_of(nocas, "no_outreach")) * 100)
out_all_pt = round(out_ref * 100)
cas_pre_above = round((1 - rate_of(pre, "no_outreach")) * 100) - out_all_pt
cas_no_below = out_all_pt - round((1 - rate_of(nocas, "no_outreach")) * 100)
_gmax = max(
    NOOUT_SEGS.iterrows(),
    key=lambda t: abs(round(did_out[dims[t[1]["dimension"]] == t[1]["value"]].mean() * 100) - out_all_pt),
)[1]
_gv = round(did_out[dims[_gmax["dimension"]] == _gmax["value"]].mean() * 100) - out_all_pt
_gname = SEG_NAMES.get((_gmax["dimension"], _gmax["value"]), _gmax["value"])
seg_gap_name = f"{'above' if _gv > 0 else 'below'} everyone, for {_gname[0].lower() + _gname[1:]}"

# Significant outreach gaps the chart leaves out because the era check could not confirm them.
PLAIN = {
    ("Office level", "federal"): "Federal races",
    ("Office category", "Congressional"): "Congressional races",
    ("Office category", "City Council"): "City Council candidates",
    ("Opponents", "3+"): "races with 3+ opponents",
}
_ex = seg[
    (seg["outcome"] == "no_outreach")
    & seg["bin"].isin(["Differs by era", "Era not checkable"])
    & ~NOT_SEGMENT
].copy()
if len(_ex):
    _ex["dist"] = [
        abs(round(did_out[dims[r["dimension"]] == r["value"]].mean() * 100) - out_all_pt)
        for _, r in _ex.iterrows()
    ]
    _e = _ex.sort_values("dist").iloc[-1]
    _why = (
        "too few signed up in one era to check it"
        if _e["bin"] == "Era not checkable"
        else "the gap flips between signup eras"
    )
    _ev = did_out[dims[_e["dimension"]] == _e["value"]].mean() * 100 - out_all_pt
    excl_note = (
        f"{PLAIN.get((_e['dimension'], _e['value']), _e['value'])} sit {int(_e['dist'])} points {'above' if _ev > 0 else 'below'} everyone, "
        f"but on {int(_e['n'])} customers and {_why}, so it is not counted."
    )
else:
    excl_note = ""

# Clear results not explained by race size but with too few self-serve customers to run that check.
_unres = clear[
    (clear["race_size"] == "") & (clear["cas"] == "self-serve too few") & ~NOT_SEGMENT.loc[clear.index]
]


def _unres_txt(r):
    name = PLAIN.get((r["dimension"], r["value"]), f"{r['dimension']}: {r['value']}")
    if r["outcome"] == "median_texts":
        return (
            f"{name} send {'more' if r['gap'] > 1 else 'fewer'} texts (median {r['gap']:.2f} times the rest)"
        )
    if r["outcome"] == "over_5000":
        return f"{name} go over 5,000 texts {'more' if r['gap'] > 0 else 'less'} often ({r['gap'] * 100:+.0f} points)"
    if O[r["outcome"]].kind == "rate":
        return f"{name}: {SHORT[r['outcome']].lower()} {r['gap'] * 100:+.0f} points"
    return f"{name}: {SHORT[r['outcome']].lower()} {r['gap']:.2f} times the rest"


unresolved = (
    "Two cases stay open because too few customers without CaS contact fall in them to check: "
    if len(_unres) == 2
    else f"{len(_unres)} cases stay open because too few customers without CaS contact fall in them to check: "
)
unresolved = (
    unresolved + " and ".join(_unres_txt(r) for _, r in _unres.iterrows()) + "." if len(_unres) else ""
)
# What a typical segment comparison of median texts could detect: the median upper end of its interval.
_mt = seg[(seg["outcome"] == "median_texts") & (seg["value"] != "unknown")]
power_x = f"{_mt['hi'].median():.1f}"

# The CaS outreach gap within each stratum that could undercut it (both groups at least MIN_N).
_lead_q = pd.qcut((ws["ged"] - ws["first_pay_date"]).dt.days, 4, labels=False, duplicates="drop")
_cg = []
for _key in [ws["signup_era"], ws["band"].astype(str).where(ws["band"].astype(str) != "unknown"), _lead_q]:
    for _, _sub in ws.groupby(_key):
        a, b = (
            _sub[_sub["cas_support"] == "CaS before outreach"],
            _sub[_sub["cas_support"] == "no CaS contact"],
        )
        if len(a) >= S.MIN_N and len(b) >= S.MIN_N:
            _cg.append(
                round((1 - a["no_outreach"].astype(bool).mean()) * 100)
                - round((1 - b["no_outreach"].astype(bool).mean()) * 100)
            )
cas_strat = f"{min(_cg)} to {max(_cg)} points across {len(_cg)} groups"

_states = ws["state"].value_counts()
_big_states = _states[_states >= S.MIN_N]
state_note = (
    f"{len(_big_states)} states have {S.MIN_N} or more customers; the share who did any outreach among them runs from "
    f"{min(did_out[ws['state'] == st].mean() for st in _big_states.index):.0%} to "
    f"{max(did_out[ws['state'] == st].mean() for st in _big_states.index):.0%}."
)

act_k = kt[kt["activated"]]
act_tx_no = act_k[act_k["cas_support"] == "no CaS contact"]["texts_sent"]
act_tx_eng = act_k[act_k["cas_support"] != "no CaS contact"]["texts_sent"]
_act = ws[ws["activated"]]
_act_gap = S.gap(O["median_texts"], _act, _act["cas_support"] == "no CaS contact")
TX_MAX = 10000


def tx_row(label, s):
    med = s.median()
    tip = f"{label}: median {med:,.0f} texts | {len(s)} activated texters with a known count"
    return (
        f'<div class="lab">{esc(label)}</div><div class="track bar" title="{esc(tip)}"><i class="bar" style="width:{min(med, TX_MAX) / TX_MAX * 100:.1f}%"></i></div>'
        f'<div class="val" title="{esc(tip)}">{med:,.0f}</div><div class="val wide small">{len(s)} people</div>'
    )


tx_hero = (
    '<figure class="dots"><h3 class="ctitle">Median texts sent, activated customers who texted</h3>'
    '<div class="scroll"><div class="rows hero vol">'
    + tx_row("No CaS contact", act_tx_no)
    + tx_row("Any CaS contact", act_tx_eng)
    + '</div></div><div class="axis-note">Six months up to election day, texters with a known count. Bars run 0 to 10,000 texts.</div></figure>'
)

# Office results within ICP and non-ICP offices: how much of each office gap is left once ICP is held fixed.
ws_icp = ws.assign(_icp=dims["ICP office"])
icp_office_rows = []
for _, r in (
    survive[survive["dimension"].isin(["Office level", "Office category"])]
    .sort_values(["dimension", "value"])
    .iterrows()
):
    o = O[r["outcome"]]
    share = S.stratified_share(
        o,
        ws_icp,
        dims[r["dimension"]] == r["value"],
        r["gap"] if o.kind == "rate" else np.log(r["gap"]),
        "_icp",
    )
    icp_office_rows.append(
        [esc(r["dimension"]), esc(r["value"]), esc(SHORT[o.key]), esc(S.fmt_gap(r)), f"{share:.0%}"]
    )
icp_office_tbl = table(
    ["Dimension", "Value", "Outcome", "Group vs rest", "Gap left within ICP and non-ICP offices"],
    icp_office_rows,
    num_from=4,
)


def fmt_row(r):
    return S.fmt_gap(r)


def surv_rows(df):
    return [
        [
            esc(r["dimension"]),
            esc(r["value"]),
            esc(SHORT[r["outcome"]]),
            f"{int(r['n'])}",
            esc(fmt_row(r)),
            f"{r['self_serve_share']:.2f}",
        ]
        for _, r in df.sort_values(["dimension", "value"]).iterrows()
    ]


surv_hdr = [
    "Dimension",
    "Value",
    "Outcome",
    "People",
    "Group vs rest (gap, 95% interval)",
    "Share of gap left among self-serve",
]
surv_out_tbl = table(surv_hdr, surv_rows(survive_out), num_from=3)
surv_feat_tbl = table(surv_hdr, surv_rows(survive_feat), num_from=3)

clear_rows = [
    [
        esc(r["dimension"]),
        esc(r["value"]),
        esc(SHORT[r["outcome"]]),
        f"{int(r['n'])}",
        esc(fmt_row(r)),
        f"{r['q']:.3f}",
        esc(" | ".join(t for t in [r["race_size"], r["cas"]] if t) or "holds"),
    ]
    for _, r in clear.sort_values(["dimension", "value", "q"]).iterrows()
]
clear_tbl = table(
    ["Dimension", "Value", "Outcome", "People", "Group vs rest", "q", "Checks"], clear_rows, num_from=3
)

seg_m = seg[~NOT_SEGMENT]
summ = (
    seg_m.assign(
        clear=seg_m["bin"] == "Clear",
        beyond=(seg_m["bin"] == "Clear") & (seg_m["race_size"] == ""),
        selfserve=(seg_m["bin"] == "Clear") & (seg_m["race_size"] == "") & (seg_m["cas"] == ""),
        toofew=(seg_m["bin"] == "Clear")
        & (seg_m["race_size"] == "")
        & (seg_m["cas"] == "self-serve too few"),
    )
    .groupby("dimension")[["clear", "beyond", "selfserve", "toofew"]]
    .sum()
    .reindex([d for d in dims if d != "Pro lead time"])
)
summ_tbl = table(
    [
        "Dimension",
        "Clear",
        "Not explained by race size",
        "Also present among self-serve customers",
        "Self-serve too few to check",
    ],
    [
        [esc(d), f"{int(r.clear)}", f"{int(r.beyond)}", f"{int(r.selfserve)}", f"{int(r.toofew)}"]
        for d, r in summ.iterrows()
    ],
)
det_tbl = table(
    ["Group size", *[f"{n}" for n in det]], [["Smallest reliable gap"] + [f"{v} pts" for v in det.values()]]
)
ov = S.overlap(dims_y)
ov_cols = list(ov.columns)
ov_tbl = table(
    ["", *ov_cols],
    [[esc(i)] + ["" if pd.isna(v) else f"{v:.2f}" for v in r.values] for i, r in ov.iterrows()],
)

# Robustness: Clear results that survive both checks, without future elections and among activated.
rob_rows = []
for _, r in survive_out.iterrows():
    o = O[r["outcome"]]
    lab = dims[r["dimension"]]
    cells = [esc(r["dimension"]), esc(r["value"]), esc(SHORT[o.key]), esc(fmt_row(r))]
    for mask in [~ws["future_election"], ws["activated"]]:
        sub = ws[mask]
        gg = S.gap(o, sub, (lab == r["value"]).loc[sub.index])
        cells.append(
            "too few" if gg is None else esc(S.fmt_gap(pd.Series({**gg, "kind": o.kind, "outcome": o.key})))
        )
    rob_rows.append(cells)
rob_tbl = table(
    ["Dimension", "Value", "Outcome", "All", "Without races not yet held", "Activated only"],
    rob_rows,
    num_from=99,
)

# Every Clear segment result re-run among activated: still clear, same direction only, reversed, or too few.
act = ws[ws["activated"]]
act_tally = {"holds": 0, "same": 0, "reverses": 0, "few": 0}
for _, r in clear.iterrows():
    o = O[r["outcome"]]
    gg = S.gap(o, act, (dims[r["dimension"]] == r["value"]).loc[act.index])
    if gg is None:
        act_tally["few"] += 1
        continue
    null = 0 if o.kind == "rate" else 1
    same = (gg["gap"] - null) * (r["gap"] - null) > 0
    act_tally["holds" if same and not gg["lo"] <= null <= gg["hi"] else "same" if same else "reverses"] += 1

# CaS groups among activated: does the gap survive once everyone has reached voters at least once?
act_cas_tbl = table(
    [
        "CaS group",
        "Activated | count",
        "Texted, among activated | count",
        "Paid for outreach, among activated | count",
    ],
    [
        [
            esc(CAS_LABEL[c]),
            f"{rate_of(by_cas[c], 'activated'):.0%} {SEP} {kn(by_cas[c], 'activated')}",
            *(
                f"{rate_of(a, col):.0%} {SEP} {kn(a, col)}"
                for a in [by_cas[c][by_cas[c]["activated"]]]
                for col in ["texted", "paid"]
            ),
        ]
        for c in CAS_ORDER
    ],
)

# ---------- appendix ----------
brief_text = (PROJ / "win_pro_pricing_segments_brief.yaml").read_text()
brief = yaml.safe_load(brief_text)


def para(v):
    return "".join(f"<p>{esc(x.strip())}</p>" for x in str(v).strip().split("\n\n"))


FRAMING = [
    ("Decision", brief["decision"]["what_action"] + "\n\nWho acts: " + brief["decision"]["who_acts"]),
    ("Question", brief["question"]["one_sentence"]),
    ("Hypothesis", brief["question"]["underlying_hypothesis"]),
    ("Population", brief["population"]["included"] + "\n\nExcluded: " + brief["population"]["excluded"]),
    (
        "Dimensions",
        brief["dimensions"]["rule"] + "\n\nDropped in framing: " + brief["dimensions"]["dropped_in_framing"],
    ),
    ("Outcomes", brief["target"]["definition"]),
    ("Method", brief["comparison"]["method"]),
    ("Window", brief["observation_window"]["boundary_semantics"]),
    ("What would change the answer", brief["falsification"]["what_would_change_belief"]),
    ("Known concerns", "\n\n".join(c.strip() for c in brief["known_concerns"])),
]
framing_html = "".join(f"<dt>{esc(k)}</dt><dd>{para(v)}</dd>" for k, v in FRAMING)

ASSUMPTIONS = [
    (
        "Population: the DATA-2809 paying Pro customers with a 2026 election, built by the same code.",
        "One population across the two analyses, so their numbers line up.",
        f"{N} customers.",
        "Tristan, framing",
    ),
    (
        "Only dimensions known when someone buys Pro; contested vs uncontested kept with a flag.",
        "A pricing segment has to be identifiable at the point of sale.",
        "Usage groups, team use, activation and activity are outcomes, not segments.",
        "Tristan, framing",
    ),
    (
        "No fixed effect-size bar. Gaps sorted into Clear / No difference, with race size as the yardstick and gaps in pricing units.",
        "A fixed 10-point bar was arbitrary and, with these group sizes, would have failed almost everything for lack of data.",
        f"{int(counts.get('Clear', 0))} Clear of {n_tests} comparisons.",
        "Tristan, framing",
    ),
    (
        "Clear means a false-discovery-corrected q under 0.10 across the grid and the same direction in both signup eras.",
        f"{n_tests} one-vs-rest tests; some would look real by chance.",
        "A Possible bin (same direction, not significant) was dropped: a null result points the same way in both eras about half the time.",
        "Analyst, framing; Possible dropped by Tristan after review",
    ),
    (
        "Added office category, onboarding-to-Pro days, number of opponents, single vs multi-seat, acquisition source.",
        "The wide user table has more purchase-time fields than the first list.",
        "Five more dimensions.",
        "Tristan, framing",
    ),
    (
        "Pro lead time reads the first subscription payment, not pro_since.",
        f"pro_since is blank for exactly the {n_canceled} customers who canceled after their election, so its unknown bucket would have been the canceled group.",
        "Lead time known for everyone.",
        "Analyst, execution",
    ),
    (
        "Number of opponents counts opponents (0 = uncontested); contested vs uncontested shown as counts only.",
        f"Only {n_uncontested} customers are uncontested.",
        "Contested cannot be tested.",
        "Analyst, execution",
    ),
    (
        "Offline acquisition source labelled as sales.",
        f"It is exactly the {n_sales} sales-sourced customers.",
        "Repeats sales vs marketing on one value.",
        "Analyst, execution",
    ),
    (
        "CaS support added as a stratifier: the results-coverage rule (1+ call, or 2+ emails or texts) plus meetings, from a Campaign Success owner.",
        "ICP looked like a result of CaS support rather than a customer type.",
        "Three groups: none, before any outreach, only after outreach began.",
        "Tristan, results checkpoint",
    ),
    (
        "CaS contact read as engagement, partly the customer's own choice.",
        "Every upgrade is auto-scheduled a CaS call; not everyone takes it.",
        "CaS results are association, not proof CaS causes outreach.",
        "Tristan, results checkpoint",
    ),
    (
        "Each Clear result checked among self-serve customers (no CaS contact) instead of averaged across CaS groups.",
        "CaS interacts with segments: gaps appear only among the engaged, so an average hid it.",
        f"ICP: {icp_clear} Clear results, {icp_left} also present among self-serve.",
        "Analyst, execution",
    ),
    (
        "Significant results that are not Clear kept apart as 'differs by era' or 'era not checkable', and Clear results with too few self-serve customers marked unresolved.",
        "Methodology review: filing them as 'no difference' hid large gaps, and 'too few to check' was being read as 'failed the check'.",
        f"{int(counts.get('Differs by era', 0))} differ by era, {int(counts.get('Era not checkable', 0))} not checkable, {len(_unres)} unresolved.",
        "Tristan, after reviews",
    ),
    (
        "CaS contact dated when it met the rule (first call or meeting, or second email or text), not at the first touch; texts ordered against the first text, not any send.",
        "Methodology review: the first touch could come before the contact that counts.",
        f"{len(pre)} customers CaS reached before outreach, {len(post)} only after.",
        "Tristan, after reviews",
    ),
    (
        "Over-5,000 ceiling also counts texters with a known count plus sends no source counted.",
        "Methodology review: those texters could be over too.",
        "Ceilings rise a few points on both pages.",
        "Tristan, after reviews",
    ),
    (
        "State shown as background, not tested as a segment.",
        "Nobody would price by state, and its gaps tracked single channels.",
        "Fewer comparisons in the correction.",
        "Tristan, after reviews",
    ),
    (
        "Main body worded as association, not cause, for CaS; text-volume gap shown with its range.",
        "Product review: the answer read as causal while the limits said it was not.",
        "Answer and recommendation 3 reworded.",
        "Tristan, after reviews",
    ),
]
assump_tbl = (
    '<div class="scroll"><table class="wrap"><thead><tr><th>Decision</th><th>Why</th><th>Effect</th><th>Who</th></tr></thead><tbody>'
    + "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in ASSUMPTIONS)
    + "</tbody></table></div>"
)


def code(title, text, lang="sql"):
    body = inspect.cleandoc(text) if lang == "py" else text.strip()
    return (
        f'<details class="code"><summary>{esc(title)}</summary><pre><code>{esc(body)}</code></pre></details>'
    )


seg_sql = BS.segments_sql(str(ws["run_ts"].iloc[0]))

# ---------- page ----------
TEMPLATE = (PROJ / "segments_page_template.html").read_text()
fills = {
    "RUN": esc(str(RUN)),
    "N": f"{N}",
    "N_FEAT": f"{int(ws['feature_measured'].sum())}",
    "SEP": SEP,
    "NOCAS_N": f"{len(nocas)}",
    "PRE_N": f"{len(pre)}",
    "POST_N": f"{len(post)}",
    "NOCAS_TEXT": pct(rate_of(nocas, "texted")),
    "NOCAS_PAID": pct(rate_of(nocas, "paid")),
    "NOCAS_NOOUT": pct(rate_of(nocas, "no_outreach")),
    "ACT_CAS_TBL": act_cas_tbl,
    "ACT_NOCAS": pct(rate_of(nocas, "activated")),
    "ACT_PRE": pct(rate_of(pre, "activated")),
    "ACT_NOCAS_TEXT": pct(rate_of(nocas[nocas["activated"]], "texted")),
    "ACT_PRE_TEXT": pct(rate_of(pre[pre["activated"]], "texted")),
    "ACT_NOCAS_PAID": pct(rate_of(nocas[nocas["activated"]], "paid")),
    "ACT_PRE_PAID": pct(rate_of(pre[pre["activated"]], "paid")),
    "ACT_HOLDS": f"{act_tally['holds']}",
    "ACT_SAME": f"{act_tally['same']}",
    "ACT_REV": f"{act_tally['reverses']}",
    "ACT_FEW": f"{act_tally['few']}",
    "PRE_TEXT": pct(rate_of(pre, "texted")),
    "POST_TEXT": pct(rate_of(post, "texted")),
    "PRE_PAID": pct(rate_of(pre, "paid")),
    "ENG_N": f"{len(engaged)}",
    "ENG_TEXT": pct(rate_of(engaged, "texted")),
    "NOCAS_TX_K": f"{int((kt['cas_support'] == 'no CaS contact').sum())}",
    "NOCAS_TX_CI": f"{_tx_gap['lo']:.0%} to {_tx_gap['hi']:.0%}",
    "BIG_TX_NO": f"{_big_kt[_big_kt['cas_support'] == 'no CaS contact']['texts_sent'].median():,.0f}",
    "BIG_TX_ENG": f"{_big_kt[_big_kt['cas_support'] != 'no CaS contact']['texts_sent'].median():,.0f}",
    "MED_TX_NO": f"{med_texts_no:,.0f}",
    "MED_TX_ENG": f"{med_texts_eng:,.0f}",
    "MED_USD_NO": f"${med_usd_no:,.0f}",
    "MED_USD_ENG": f"${med_usd_eng:,.0f}",
    "BAND_LO": f"{band_med.get('under 1,000', np.nan):,.0f}",
    "BAND_HI": f"{band_med.get('10,000+', np.nan):,.0f}",
    "O5_TX": pct_range(texters),
    "O5_TX_N": f"{len(texters)}",
    "O5_BIG": pct_range(big),
    "O5_SMALL": pct_range(texters[texters["band"] == "under 1,000"]),
    "O5_ALL": pct_range(ws),
    "WHY_RANGE": esc(why_range(ws)),
    "ICP_ALL": f"{icp_all[0]:.0%} vs {icp_all[1]:.0%}",
    "ICP_PRE": f"{icp_pre[0]:.0%} vs {icp_pre[1]:.0%}",
    "ICP_NO": f"{icp_no[0]:.0%} vs {icp_no[1]:.0%}",
    "ICP_CLEAR": f"{icp_clear}",
    "ICP_LEFT": f"{icp_left}",
    "CAS_FIRST": f"{cas_first} of {len(tx)}",
    "LEAD_MED": f"{lead_med:.0f}",
    "N_TESTS": f"{n_tests}",
    "N_CLEAR": f"{int(counts.get('Clear', 0))}",
    "N_DIFFERS": f"{int(counts.get('Differs by era', 0))}",
    "N_NOTCHECK": f"{int(counts.get('Era not checkable', 0))}",
    "CAS_STRAT": cas_strat,
    "STATE_NOTE": esc(state_note),
    "POWER_X": power_x,
    "UNRESOLVED": esc(unresolved),
    "EXCL_NOTE": esc(excl_note),
    "SEG_GAP_NAME": esc(seg_gap_name),
    "CAS_PRE_ABOVE": f"{cas_pre_above}",
    "CAS_NO_BELOW": f"{cas_no_below}",
    "OUT_ALL": f"{out_all_pt}%",
    "ACT_TX_CI": f"{_act_gap['lo']:.0%} to {_act_gap['hi']:.0%}",
    "N_SURV_OUT": f"{len(survive_out)}",
    "N_SURV_FEAT": f"{len(survive_feat)}",
    "STORY_NONPART": feat_pt("Partisan", "nonpartisan", "area_story"),
    "AI_PART": feat_pt("Partisan", "partisan", "area_ai"),
    "AI_MULTI": feat_pt("Seats", "multi-seat", "area_ai"),
    "NODIFF": esc(", ".join(d[0].lower() + d[1:] for d in nodiff)) or "none",
    "TEAM": f"{team_k} of {team_n}",
    "DET_60": f"{det[60]}",
    "DET_EVEN": f"{det[N // 2]}",
    "CAS_FIG": cas_fig,
    "CAS_FEAT_FIG": cas_feat_fig,
    "VOL_TBL": vol_tbl,
    "BAND_TBL": band_tbl,
    "VOL_HERO": vol_hero,
    "FIG_CSS": FIG_CSS,
    "OUT_HERO": out_hero,
    "TX_HERO": tx_hero,
    "OUT_NO": pct(1 - rate_of(nocas, "no_outreach")),
    "OUT_PRE": pct(1 - rate_of(pre, "no_outreach")),
    "ACT_TX_NO": f"{act_tx_no.median():,.0f}",
    "ACT_TX_ENG": f"{act_tx_eng.median():,.0f}",
    "ACT_TX_NO_N": f"{len(act_tx_no)}",
    "ACT_TX_ENG_N": f"{len(act_tx_eng)}",
    "SEG_GAP_MAX": f"{seg_gap_max}",
    "CAS_GAP": f"{cas_gap}",
    "ICP_OFFICE_TBL": icp_office_tbl,
    "ICP_TBL": icp_tbl,
    "SURV_OUT_TBL": surv_out_tbl,
    "SURV_FEAT_TBL": surv_feat_tbl,
    "CLEAR_TBL": clear_tbl,
    "SUMM_TBL": summ_tbl,
    "DET_TBL": det_tbl,
    "OV_TBL": ov_tbl,
    "ROB_TBL": rob_tbl,
    "FRAMING": framing_html,
    "ASSUMP_TBL": assump_tbl,
    "CODE_SQL": code("SQL: segment and CaS columns (build_segments.py)", seg_sql),
    "CODE_CAS": code(
        "Python: CaS support rule (analyze_segments.cas_support)", inspect.getsource(S.cas_support), "py"
    ),
    "CODE_GRID": code(
        "Python: gaps, bins and checks (analyze_segments)",
        "\n".join(
            inspect.getsource(f)
            for f in [
                S.rate_gap,
                S.median_gap,
                S.era_direction,
                S.stratified_share,
                S.self_serve_share,
                S.classify,
            ]
        ),
        "py",
    ),
    "CODE_DIMS": code(
        "Python: dimension values (analyze_segments.dimensions)", inspect.getsource(S.dimensions), "py"
    ),
    "BRIEF": code("Full brief: win_pro_pricing_segments_brief.yaml", brief_text, "yaml"),
    "DATA": json.dumps(explorer, separators=(",", ":")).replace("</", "<\\/"),
    "DIM_OPTIONS": "".join(
        f'<option value="{esc(d)}"{" selected" if d == "ICP office" else ""}>{esc(d)}</option>'
        for d in DIM_ORDER
    ),
}
page = TEMPLATE
for k, v in fills.items():
    page = page.replace("{{" + k + "}}", v)
left = [t for t in __import__("re").findall(r"\{\{[A-Z_0-9]+\}\}", page)]
assert not left, f"unfilled placeholders: {left}"
out = Path(sys.argv[1])
out.write_text(page)
print(f"wrote {out} ({len(page):,} bytes)")
