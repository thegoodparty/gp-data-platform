"""Render the DATA-2809 findings page from the same analyze.py functions the notebook uses.

Run from analytics/ after build_working_set.py:
    uv run --with pyarrow --with pyyaml python projects/win_pro_votes_needed/build_page.py <out.html>

Design follows analytics/planning/journey-dashboard/design-principles.md: ink to data, sections as
questions, dot plots against an "everyone" line, rates with counts in the tooltip, faded marks under
100 people.
"""

import html
import inspect
import sys
from pathlib import Path

import yaml

PROJ = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJ))
sys.path.insert(0, str(PROJ.parents[1] / "lib"))
import analyze as A  # noqa: E402
import build_working_set as B  # noqa: E402

ws = A.load()
RUN = ws["run_ts"].iloc[0]
BANDS = [b for b in A.BANDS if b != "unknown"]
N = len(ws)
FADE_N = 100
esc = html.escape
SEP = '<span class="sep">|</span>'

for c in A.CHANNELS:
    ws[f"used_{c}"] = ws[f"sends_{c}"] > 0

before = ws[ws.signup_era == "before Jul 2026"]
after = ws[ws.signup_era == "from Jul 2026"]
ERAS = [("All Pro", ws), ("Signed up before Jul 2026", before), ("Signed up from Jul 2026", after)]
act = ws[ws.activated]
act_before = act[act.signup_era == "before Jul 2026"]
act_after = act[act.signup_era == "from Jul 2026"]
ACT_ERAS = [
    ("Activated", act),
    ("Activated, signed up before Jul 2026", act_before),
    ("Activated, signed up from Jul 2026", act_after),
]
COH4 = [
    ("All Pro", ws),
    ("Activated", act),
    ("Activated, signed up before Jul", act_before),
    ("Activated, signed up from Jul", act_after),
]
AXIS = "Each track runs 0% to 100%. Dashed line: everyone in this group. Faded: under 100 people. Hover for counts."


def code(title, text, lang="sql"):
    body = inspect.cleandoc(text) if lang == "py" else text.strip()
    return (
        f'<details class="code"><summary>{esc(title)}</summary><pre><code>{esc(body)}</code></pre></details>'
    )


def src(fn):
    return inspect.getsource(fn)


def dotrow(label, k, n, ref):
    """One band on a 0-100% track: dot, value label, dashed 'everyone' reference."""
    refline = f'<i class="ref" style="left:{ref * 100:.1f}%"></i>'
    if n < A.MIN_N:
        return (
            f'<div class="lab">{esc(label)}</div><div class="track" title="{esc(label)}: {k} of {n}, under 30 people">'
            f'{refline}</div><div class="val small">{k} of {n}</div>'
        )
    p = k / n
    faded = n < FADE_N
    tip = f"{label}: {p:.0%} | {k} of {n}" + (" | under 100 people" if faded else "")
    return (
        f'<div class="lab">{esc(label)}</div><div class="track" title="{esc(tip)}">{refline}'
        f'<i class="dot{" faded" if faded else ""}" style="left:{p * 100:.1f}%"></i></div>'
        f'<div class="val{" faded-t" if faded else ""}" title="{esc(tip)}">{p:.0%}</div>'
    )


def panel(name, d, flag):
    ref = d[flag].mean()
    rows = "".join(
        dotrow(b, int(d.loc[d.band == b, flag].sum()), int((d.band == b).sum()), ref) for b in BANDS
    )
    return (
        f'<div class="panel"><div class="ptitle">{esc(name)}</div>'
        f'<div class="pref">everyone {ref:.0%} {SEP} {int(d[flag].sum())} of {len(d)}</div>'
        f'<div class="rows">{rows}</div></div>'
    )


def fig(panels_html, cols, caption, note=AXIS):
    return (
        f'<figure class="dots"><figcaption>{caption}</figcaption>'
        f'<div class="scroll"><div class="panels" style="--cols:{cols}">{panels_html}</div></div>'
        f'<div class="axis-note">{note}</div></figure>'
    )


def table(headers, rows, num_from=1, total_last=False):
    th = "".join(f'<th class="{"num" if i >= num_from else ""}">{esc(h)}</th>' for i, h in enumerate(headers))
    body = ""
    for j, r in enumerate(rows):
        cls = ' class="total"' if total_last and j == len(rows) - 1 else ""
        body += (
            f"<tr{cls}>"
            + "".join(f'<td class="{"num" if i >= num_from else ""}">{c}</td>' for i, c in enumerate(r))
            + "</tr>"
        )
    return f'<div class="scroll"><table{' class="wrap"' if num_from == 99 else ''}><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table></div>'


# ---------- 1: votes needed ----------
cov = A.coverage(ws)
cov_tbl = table(["Source", "Users"], [[esc(i), f"{int(v)}"] for i, v in cov["users"].items()])
bs = A.band_sizes(ws)


def fmt(x):
    return "" if x != x else f"{x:,.0f}"


bs_tbl = table(
    ["Votes needed", "Users", "Median votes needed", "Median registered voters", "Nov 3 races"],
    [
        [
            esc(str(b)),
            f"{int(r['users'])}",
            fmt(r["median votes needed"]),
            fmt(r["median voter_count"]),
            f"{int(r['Nov 3 races'])}",
        ]
        for b, r in bs.iterrows()
    ],
)

# ---------- 2: features ----------
FEAT_CAP = "Share of users who acted in each area in the six months before their general election."


def feat_fig(d):
    return fig("".join(panel(A.AREA_NAMES[a], d, f"acted_{a}") for a in A.AREAS), len(A.AREAS), FEAT_CAP)


# Feature events are measurable only for races from Jul 15 (see analyze.load, feature_measured).
fm = ws[ws.feature_measured]
fm_before, fm_after = fm[fm.signup_era == "before Jul 2026"], fm[fm.signup_era == "from Jul 2026"]
fm_act = fm[fm.activated]
fm_act_before, fm_act_after = (
    fm_act[fm_act.signup_era == "before Jul 2026"],
    fm_act[fm_act.signup_era == "from Jul 2026"],
)
feat_all, feat_before, feat_after = feat_fig(fm), feat_fig(fm_before), feat_fig(fm_after)
feat_act, feat_act_before, feat_act_after = feat_fig(fm_act), feat_fig(fm_act_before), feat_fig(fm_act_after)
dep = A.feature_depth(fm)
dep_tbl = table(
    ["Votes needed", *dep.columns],
    [[esc(str(b))] + [esc(str(v)) for v in r.values] for b, r in dep.iterrows() if b != "unknown"],
)

# ---------- 3: channels ----------
CH_NAMES = ["Texting", "Robocall", "Social", "Door knocking", "Phone banking", "Channel unknown"]
CH_CAP = "Share of users with at least one committed send on the channel in the six months up to and including election day."
chan_fig = fig(
    "".join(panel(n, ws, f"used_{c}") for c, n in zip(A.CHANNELS, CH_NAMES, strict=False)),
    len(A.CHANNELS),
    CH_CAP,
)
chan_fig_act = fig(
    "".join(panel(n, act, f"used_{c}") for c, n in zip(A.CHANNELS, CH_NAMES, strict=False)),
    len(A.CHANNELS),
    CH_CAP,
)
un, tot = int(ws.sends_unattributed.sum()), int(ws.sends_total.sum())

# ---------- 4: volume and spend ----------
maxmed = max(
    float(d[d.texts_known].groupby("band", observed=True)["texts_sent"].median().max()) for _, d in COH4
)


def who_row(label, d):
    n = len(d)
    parts = [
        d.texted.sum(),
        d.tried_text_only.sum(),
        (~d.texted & ~d.tried_text_only & ~d.no_outreach).sum(),
        d.no_outreach.sum(),
    ]
    return [esc(label), f"{n}"] + [f"{int(k)} {SEP} {k / n:.0%}" for k in parts]


_other = (
    ws.sends_robocall
    + ws.sends_social
    + ws.sends_door_knocking
    + ws.sends_phone_banking
    + ws.sends_unattributed
) > 0
_gap = ws[ws.activated & ~ws.texted]
gap_n, gap_other, gap_unk = len(_gap), int(_other[_gap.index].sum()), int((_gap.sends_unattributed > 0).sum())
tex_not_act = int((~ws.activated & ws.texted).sum())


def _cell(mask):
    return f"{int(mask.sum())}"


act_tex_tbl = table(
    ["", "Texted in window", "No text in window", "Total"],
    [
        [
            "Activated",
            _cell(ws.activated & ws.texted),
            _cell(ws.activated & ~ws.texted),
            f"{int(ws.activated.sum())} {SEP} {ws.activated.mean():.0%}",
        ],
        [
            "Not activated",
            _cell(~ws.activated & ws.texted),
            _cell(~ws.activated & ~ws.texted),
            f"{int((~ws.activated).sum())}",
        ],
        [
            "Total",
            f"{int(ws.texted.sum())} {SEP} {ws.texted.mean():.0%}",
            f"{int((~ws.texted).sum())} {SEP} {(~ws.texted).mean():.0%}",
            f"{N}",
        ],
    ],
    total_last=True,
)
WHO_H = [
    "Votes needed",
    "Users",
    "Sent texts",
    "Started a text, never sent",
    "Other channels only",
    "No outreach at all",
]
who_tbl = table(
    WHO_H, [who_row(b, ws[ws.band == b]) for b in BANDS] + [who_row("All Pro", ws)], total_last=True
)


def medrow(label, d):
    tt = d[d.texts_known]
    if len(tt) == 0:
        return f'<div class="lab">{esc(label)}</div><div class="track bars"></div><div class="val">-</div>'
    m, v = tt.texts_sent.median(), tt.texts_per_vote.median()
    faded = len(tt) < FADE_N
    tip = f"{label}: median {m:,.0f} texts | {v:.2f} per vote needed | {len(tt)} texters with a known count, {int(d.texts_unknown.sum())} more unknown | {len(d)} users"
    return (
        f'<div class="lab">{esc(label)}</div><div class="track bars" title="{esc(tip)}">'
        f'<i class="barfill{" faded" if faded else ""}" style="width:{m / maxmed * 100:.1f}%"></i></div>'
        f'<div class="val wide{" faded-t" if faded else ""}" title="{esc(tip)}">{m:,.0f} {SEP} {v:.1f}/vote</div>'
    )


vol_fig = fig(
    "".join(
        f'<div class="panel"><div class="ptitle">{esc(n)}</div><div class="rows">'
        + "".join(medrow(b, d[d.band == b]) for b in BANDS)
        + "</div></div>"
        for n, d in COH4
    ),
    4,
    "Median texts per texter with a known count, then texts per vote needed. Bars share one scale across all four panels.",
    "Faded: under 100 texters. Hover for counts, including texters whose count is unknown.",
)
al = A.allowance_table(ws)
allow_tbl = table(
    ["Votes needed", *al.columns], [[esc(str(b))] + [esc(str(v)) for v in r.values] for b, r in al.iterrows()]
)
over_up = {
    b: (int(d.over_bundle.sum()), int(d.over_bundle_upper.sum()), len(d))
    for b in BANDS
    for d in [ws[ws.band == b]]
}
big = ws[ws.band.isin(["2,500-9,999", "10,000+"])]
big_act = big[big.activated]
over_fig = fig(
    "".join(panel(n, d, "over_bundle") for n, d in COH4),
    4,
    "Share of users who sent more than 5,000 texts (the proposed bundle) in the six months up to and including election day. "
    "A lower bound: texters with no known count are counted as not over.",
)
paid_fig = fig(
    "".join(panel(n, d, "paid") for n, d in COH4),
    4,
    "Share of users who paid for any outreach in the same window.",
)

p = ws[ws.paid]
top = float(p.outreach_usd.max())
tot_usd = float(p.outreach_usd.sum())
per_text = float((p.outreach_usd / p.texts_sent.where(p.texts_sent > 0)).median())
pk = p[p.texts_known]
per_text_lo = float(pk.outreach_usd.sum() / pk.texts_sent.sum())
cost_5k = f"${per_text_lo * 5000:,.0f} to ${per_text * 5000:,.0f}"
q = p.outreach_usd.quantile([0.25, 0.5, 0.75, 0.9])


def spend_row(label, d):
    pp = d[d.paid]
    if len(pp) == 0:
        return [esc(label), f"0 of {len(d)}", "-", "-", "$0"]
    return [
        esc(label),
        f"{len(pp)} of {len(d)}",
        f"${pp.outreach_usd.median():,.0f}",
        f"${pp.outreach_usd.quantile(.25):,.0f} to ${pp.outreach_usd.quantile(.75):,.0f}",
        f"${d.outreach_usd.sum():,.0f}",
    ]


spend_tbl = table(
    ["Votes needed", "Paid", "Median $ per payer", "Middle half of payers", "Total $"],
    [spend_row(b, ws[ws.band == b]) for b in BANDS] + [spend_row("All Pro", ws)],
    total_last=True,
)


# ---------- 5: robustness ----------
def mini(d):
    v = A.volume_table(d)
    return [
        [
            esc(str(b)),
            f"{r['users']}",
            esc(r["median texts (texters)"]),
            esc(r["over 5,000 texts"]),
            esc(r["median outreach $ (payers)"]),
        ]
        for b, r in v.iterrows()
        if b != "unknown"
    ]


ROB_H = ["Votes needed", "Users", "Median texts (texters)", "Over 5,000 texts", "Median $ (payers)"]
rob_early = table(ROB_H, mini(ws[~ws.early_election]), total_last=True)
rob_future = table(ROB_H, mini(ws[~ws.future_election]), total_last=True)
b10 = ws[ws.band == "10,000+"]
lvl = b10.groupby("office_level").agg(
    users=("user_id", "size"),
    med=("votes_needed", "median"),
    texted=("texted", "sum"),
    over=("over_bundle", "sum"),
)
lvl_tbl = table(
    ["Office level", "Users", "Median votes needed", "Texted", "Over 5,000 texts"],
    [
        [esc(i), f"{int(r.users)}", f"{r.med:,.0f}", f"{int(r.texted)}", f"{int(r.over)}"]
        for i, r in lvl.iterrows()
    ],
)


def status_row(label, d):
    n = len(d)
    early = int(d.early_election.sum())
    return [
        esc(label),
        f"{n}",
        f"{early} {SEP} {early / n:.0%}",
        f"{int(d.texted.sum())} {SEP} {d.texted.mean():.0%}",
        f"{int(d.over_bundle.sum())} {SEP} {d.over_bundle.mean():.0%}",
        f"{int(d.activated.sum())} {SEP} {d.activated.mean():.0%}",
        f"{int(d.no_outreach.sum())} {SEP} {d.no_outreach.mean():.0%}",
    ]


status_tbl = table(
    [
        "Pro status",
        "Users",
        "Election before Jul 15",
        "Sent texts",
        "Over 5,000 texts",
        "Activated",
        "No outreach",
    ],
    [status_row(k, ws[ws.pro_status == k]) for k in ["Pro today", "canceled after election"]]
    + [status_row("All Pro", ws)],
    total_last=True,
)


def ci_cell(d, flag):
    k, n = int(d[flag].sum()), len(d)
    if n < A.MIN_N:
        return f"{k} of {n}"
    lo, hi = A.wilson(k, n)
    return f"{k / n:.0%} {SEP} {lo:.0%} to {hi:.0%} {SEP} {k} of {n}"


ci_tbl = table(
    [
        "Votes needed",
        "Over 5,000 texts, all Pro",
        "Over 5,000 texts, activated",
        "Acted in outreach (races from Jul 15)",
        "No outreach at all, all Pro",
    ],
    [
        [
            esc(b),
            ci_cell(ws[ws.band == b], "over_bundle"),
            ci_cell(act[act.band == b], "over_bundle"),
            ci_cell(fm[fm.band == b], "acted_outreach"),
            ci_cell(ws[ws.band == b], "no_outreach"),
        ]
        for b in BANDS
    ],
)
canc = ws[ws.pro_status == "canceled after election"]
canc_on_nov3 = ws[(ws.pro_status == "canceled after election") & ~ws.early_election]
n_early, n_future = int(ws.early_election.sum()), int(ws.future_election.sum())

# ---------- appendix ----------
brief_text = (PROJ / "win_pro_votes_needed_brief.yaml").read_text()
brief = yaml.safe_load(brief_text)


def para(v):
    return "".join(f"<p>{esc(x.strip())}</p>" for x in str(v).strip().split("\n\n"))


FRAMING = [
    ("Decision", brief["decision"]["what_action"] + "\n\nWho acts: " + brief["decision"]["who_acts"]),
    ("Question", brief["question"]["one_sentence"] + "\n\nType: " + brief["question"]["type"]),
    ("Hypothesis", brief["question"]["underlying_hypothesis"]),
    ("Population", brief["population"]["included"] + "\n\nExcluded: " + brief["population"]["excluded"]),
    ("Votes needed", brief["votes_needed"]["definition"]),
    ("Bands", brief["votes_needed"]["bands"]),
    ("Measures", brief["target"]["definition"]),
    (
        "Window",
        brief["observation_window"]["start"]
        + "\n\nEnd: "
        + brief["observation_window"]["end"]
        + "\n\n"
        + brief["observation_window"]["boundary_semantics"],
    ),
    ("Comparison", brief["comparison"]["notes"]),
    ("What would change the answer", brief["falsification"]["what_would_change_belief"]),
    ("Known concerns", "\n\n".join(c.strip() for c in brief["known_concerns"])),
]
framing_html = "".join(f"<dt>{esc(k)}</dt><dd>{para(v)}</dd>" for k, v in FRAMING)

known = N - int((ws.wn_source == "none").sum())
n_comped = 35  # never-paid Pro, measured in the 2026-11-12 re-frame (see brief amendments)
n_nogen = int(ws.ged_from_campaign.sum())
ASSUMPTIONS = [
    (
        "Population: paying Pro customers whose election is in 2026.",
        "Pricing applies to every Pro customer. The usage-groups work (DATA-2690) only covered July-on signups.",
        "Older Pro users are in, and they text less. Shown split by signup era.",
        "Tristan, framing",
    ),
    (
        "Pro means Pro today, or a subscription that ended on or after election day.",
        "The first run used Pro today only, which dropped everyone who canceled once their race was over. People who canceled before their election are left out on purpose: they read as not serious about the race, and their window was not a Pro window.",
        f"Adds {len(canc)} people, {int(canc.early_election.sum())} of them with spring races. Shown as a check in Section 5.",
        "Tristan, re-frame",
    ),
    (
        "Comped Pro (never made a subscription payment) is excluded.",
        "Pricing is about paying customers.",
        f"{n_comped} people, all Pro today. Including them moves rates by 1 to 2 points and changes no finding.",
        "Tristan, re-frame",
    ),
    (
        "Sources: the marts first (user_journey, user_journey_events, mart_civics); upstream only where no mart has the field.",
        "Marts are shaped and tested for this. Upstream reads: projected turnout, the DATA-2690 feature events, and unsent text campaigns.",
        "Send counts are identical to the earlier intermediate-table source (3,470 sends, 8.1M texts).",
        "Tristan, re-frame",
    ),
    (
        f"Candidacies with a 2026 election date but no separate general date are in ({n_nogen} people).",
        "The first run included them the same way; most have no outside race match, so only one date is known.",
        "Their date is used as the election date for the window.",
        "Re-frame",
    ),
    (
        "One race per user: the candidacy on user_journey.",
        "Bands need one race per person; user_journey is one row per user.",
        "A user running twice in 2026 is counted on their latest candidacy.",
        "Framing",
    ),
    (
        "Votes needed is rebuilt with gp-api's live rule (half the projected turnout plus one).",
        "gp-api stores no number; the stored win_number columns are empty for 2026.",
        f"{known} of {N} covered. It is today's projection: in the first run, 94% were within 10% of the value shown at onboarding.",
        "Tristan, framing",
    ),
    (
        "Bands: under 1,000 / 1,000 to 2,499 / 2,500 to 9,999 / 10,000 and over.",
        "Fixed before the run so results could not pick them.",
        "The 10,000+ band mixes city and federal races; read it as big races only.",
        "Tristan, framing",
    ),
    (
        "Window: six months before the election, up to and including election day.",
        "Six months holds 88 to 99% of sends (DATA-2477). Election day added after the first readout for get-out-the-vote texts.",
        "Election day added 1 texter in the first run.",
        "Tristan, amendment",
    ),
    (
        "A texter is anyone with at least one sent text.",
        "The first version required a known text count and missed 66 texters (238 instead of 305). Tristan questioned the result.",
        f"Text volume is measured only where the count is known; {int(ws.texts_unknown.sum())} texters have no count.",
        "Tristan, amendment",
    ),
    (
        "Activated users are shown beside all Pro users, using the governed definition.",
        "To read behavior among people who actually use the product. Governed: reached voters at least once, any channel, ever.",
        "Activation requires outreach, so outreach shares among activated users are high by construction.",
        "Tristan, amendment",
    ),
    (
        "Spend uses the Revenue dashboard rule: outreach purchases minus refunds, main Stripe customer record.",
        "One definition of revenue across reports.",
        "Only texting is paid; other channels are reported as use.",
        "Tristan, framing",
    ),
    (
        "Pro costs $10 a month.",
        "Checked in Stripe: every 2026 subscription payment is $10.00, all monthly. The $99 in the ask is not a live price.",
        "Text spend, not the subscription, is where users differ.",
        "Tristan, confirmed",
    ),
    (
        f"Feature use is measured on races from July 15 only ({len(fm)} of {N}).",
        "Methodology review: before then the action events record nothing in outreach, story or opponent research (older event names), though people in those races texted.",
        "The small band, a third of it spring races, no longer looks less active by construction. Volume and spend keep everyone.",
        "Review, amendment",
    ),
    (
        "Headline bundle shares use all paying Pro customers; activated is shown second.",
        "Both reviews: the bundle price applies to every customer, and activation requires outreach, so activated-only shares overstate demand.",
        f"Over 5,000 texts at 2,500+ votes: {big.over_bundle.mean():.0%} of all Pro, {big_act.over_bundle.mean():.0%} of activated.",
        "Review, amendment",
    ),
    (
        "Over 5,000 texts is a lower bound.",
        "Texters with no known count are counted as not over.",
        "An upper bound counting them all as over is shown beside it; the gap is widest under 1,000 votes.",
        "Review, amendment",
    ),
    (
        "A text costs about 3.1 to 3.5 cents.",
        "Total dollars over total texts gives 3.1; the median payer's own rate gives 3.5.",
        f"5,000 texts cost about {cost_5k}.",
        "Review",
    ),
    (
        "Measured demand is demand at a per-text price.",
        "Inside a bundle extra texts are free, so people would likely send more.",
        "The allowance table probably understates what a bundle would be asked to cover; not sized here.",
        "Review",
    ),
    (
        'Keep the company-wide term "activated", defined where it first appears.',
        "It is the term used across GoodParty; a new label would confuse more than help.",
        "",
        "Tristan",
    ),
    (
        "Medians, not totals.",
        f"One statewide race is {top / tot_usd:.0%} of all outreach dollars.",
        "Totals are shown but should not be compared across bands.",
        "Execution",
    ),
    (
        "Feature use uses the DATA-2690 action events, machine-sent events removed.",
        "One definition of feature use across analyses. The mart alternative (user_feature_use) has no per-window action-days.",
        "Views alone do not count as use.",
        "Tristan, framing",
    ),
    (
        "Under 30 people: counts only. Under 100: faded.",
        "Small groups swing; the reader should see that.",
        "",
        "Framing",
    ),
    (
        "These are associations.",
        "Bigger races also differ in office level, signup era and experience.",
        "Each headline difference is checked within signup era; not a causal claim.",
        "Framing",
    ),
]
assump_tbl = table(
    ["Assumption or decision", "Why", "Effect on the results", "Decided by"],
    [[esc(a), esc(b), esc(c), esc(d)] for a, b, c, d in ASSUMPTIONS],
    num_from=99,
)

wsql = B.working_set_sql(RUN)
asql = B.area_days_sql(RUN)
pop_sql = wsql.split("sends as (")[0]
sends_sql = "sends as (" + wsql.split("sends as (")[1].split("money as (")[0]
money_sql = "money as (" + wsql.split("money as (")[1].split("attempts as (")[0]

kt = ws[ws.texts_known]
mt_small, mt_big = (
    kt[kt.band == "under 1,000"].texts_sent.median(),
    kt[kt.band == "10,000+"].texts_sent.median(),
)
tv_small, tv_big = (
    kt[kt.band == "under 1,000"].texts_per_vote.median(),
    kt[kt.band == "10,000+"].texts_per_vote.median(),
)
aa = act[act.signup_era == "from Jul 2026"]
ob_aa = {b: (int(aa[aa.band == b].over_bundle.sum()), int((aa.band == b).sum())) for b in BANDS}
med_pay = {b: p[p.band == b].outreach_usd.median() for b in BANDS}
opp_after = {b: fm_after[fm_after.band == b].acted_opponents.mean() for b in BANDS}
out_fm = {b: fm[fm.band == b].acted_outreach.mean() for b in BANDS}
tex75 = {b: al.loc[b, "texters 75%"] for b in BANDS}
all75 = {b: al.loc[b, "all Pro 75%"] for b in BANDS}
no_out_big = ws[ws.band == "10,000+"].no_outreach
ob_mid = f"{act[act.band == '2,500-9,999'].over_bundle.mean():.0%}"
ob_big = f"{act[act.band == '10,000+'].over_bundle.mean():.0%}"
ob_small_k, ob_small_n = (
    int(act[act.band == "under 1,000"].over_bundle.sum()),
    int((act.band == "under 1,000").sum()),
)
small_med = p[p.band == "under 1,000"].outreach_usd.median()
big_med = p[p.band == "10,000+"].outreach_usd.median()
known = N - int((ws.wn_source == "none").sum())

page = f"""<title>Pro Usage by Race Size</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>
/* Layout: one padded reading column; hairline rules group sections, no boxes. GoodParty dashboard look. */
:root {{
  --bg: #ffffff; --fg: #0B1529; --muted: #5a6478; --faint: #e3e8f0; --rule: #cfd6e2;
  --blue: #0048C2; --ref: #8a93a5; --code-bg: #f4f6fa;
  --sans: "Outfit", system-ui, -apple-system, "Segoe UI", sans-serif;
  --mono: "IBM Plex Mono", ui-monospace, Menlo, monospace;
}}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{
  --bg: #0B1529; --fg: #e8edf6; --muted: #9aa6bb; --faint: #1e2a42; --rule: #2c3a55;
  --blue: #7FA8F0; --ref: #8390a8; --code-bg: #08101f; color-scheme: dark }} }}
:root[data-theme="dark"] {{
  --bg: #0B1529; --fg: #e8edf6; --muted: #9aa6bb; --faint: #1e2a42; --rule: #2c3a55;
  --blue: #7FA8F0; --ref: #8390a8; --code-bg: #08101f; color-scheme: dark }}
body {{ background: var(--bg); color: var(--fg); font: 15px/1.55 var(--sans); }}
main {{ max-width: 1080px; margin: 0 auto; padding-block: 28px 64px;
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px));
  display: flex; flex-direction: column; gap: 44px; }}
.eyebrow {{ font: 500 12px/1 var(--sans); letter-spacing: .05em; text-transform: uppercase; color: var(--muted); }}
h1 {{ font: 600 clamp(26px, 3.6vw, 36px)/1.15 var(--sans); margin: 0; text-wrap: balance; }}
h2 {{ font: 600 22px/1.25 var(--sans); margin: 0; text-wrap: balance; }}
h3 {{ font: 600 16px/1.3 var(--sans); margin: 0; }}
p {{ margin: 0; }}
header {{ display: flex; flex-direction: column; gap: 10px; }}
.meta {{ font-size: 13px; color: var(--muted); max-width: none; }}
.sep {{ color: var(--muted); opacity: .6; margin: 0 .3em; font-weight: 400; }}
section {{ display: flex; flex-direction: column; gap: 18px; border-top: 1px solid var(--rule); padding-top: 18px; }}
section[id] {{ scroll-margin-top: 16px; }}
.takeaway {{ font-size: .88rem; max-width: none; }}
.sub {{ display: flex; flex-direction: column; gap: 10px; }}
.answer {{ font: 600 clamp(18px, 2.3vw, 21px)/1.4 var(--sans); text-wrap: pretty; }}
ul.support {{ list-style: none; margin: 0; padding: 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 12px 36px; }}
ul.support li {{ display: flex; flex-direction: column; gap: 2px; min-width: 0; font-size: 14px; }}
ul.support a, nav.toc a {{ color: var(--blue); text-decoration: none; font-weight: 500; }}
ul.support a {{ font-size: 12px; }}
ul.support a:hover, nav.toc a:hover {{ text-decoration: underline; }}
nav.toc {{ display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: 13px; }}
a:focus-visible, summary:focus-visible {{ outline: 2px solid var(--blue); outline-offset: 2px; }}
figure.dots {{ margin: 0; display: flex; flex-direction: column; gap: 10px; }}
figcaption, .axis-note {{ font-size: 12.5px; color: var(--muted); }}
.scroll {{ overflow-x: auto; min-width: 0; }}
.panels {{ display: grid; grid-template-columns: repeat(var(--cols), minmax(150px, 1fr)); gap: 0 28px; min-width: calc(var(--cols) * 165px); }}
.panel {{ display: flex; flex-direction: column; gap: 4px; }}
h3.group {{ font-size: 18px; padding-top: 6px; }}
.ptitle {{ font: 600 13px/1.3 var(--sans); }}
.pref {{ color: var(--muted); font-size: 11px; margin-bottom: 4px; }}
.rows {{ display: grid; grid-template-columns: 66px 1fr 34px; align-items: center; row-gap: 9px; column-gap: 8px; }}
.rows .lab {{ color: var(--muted); font-size: 11px; white-space: nowrap; }}
.rows .val {{ font-size: 11px; font-variant-numeric: tabular-nums; text-align: right; white-space: nowrap; }}
.rows .val.small {{ color: var(--muted); }}
.track {{ position: relative; height: 14px; }}
.track::before {{ content: ""; position: absolute; left: 0; right: 0; top: 6px; height: 2px; background: var(--faint); border-radius: 1px; }}
.track .ref {{ position: absolute; top: 0; bottom: 0; border-left: 1.5px dashed var(--ref); }}
.track .dot {{ position: absolute; top: 2px; width: 10px; height: 10px; margin-left: -5px; border-radius: 50%; background: var(--blue); box-shadow: 0 0 0 2px var(--bg); }}
.dot.faded, .barfill.faded {{ opacity: .45; }}
.faded-t {{ color: var(--muted); }}
.track.bars::before {{ top: 3px; height: 8px; }}
.barfill {{ position: absolute; left: 0; top: 3px; height: 8px; background: var(--blue); border-radius: 0 4px 4px 0; }}
.rows:has(.val.wide) {{ grid-template-columns: 66px 1fr 92px; }}
table {{ border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; font-size: 13px; }}
th, td {{ padding: 6px 10px; border-bottom: 1px solid var(--faint); text-align: left; white-space: nowrap; }}
thead th {{ font-weight: 500; font-size: 12px; color: var(--muted); border-bottom: 1px solid var(--rule); white-space: normal; vertical-align: bottom; }}
.num {{ text-align: right; }}
table.wrap td {{ white-space: normal; vertical-align: top; min-width: 140px; }}
.status {{ font-size: 13px; }}
.status b {{ color: var(--fg); }}
tr.total td {{ font-weight: 600; border-top: 1px solid var(--rule); }}
.grid2 {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 24px 36px; }}
.grid2 > * {{ min-width: 0; display: flex; flex-direction: column; gap: 8px; }}
.note {{ font-size: 13px; color: var(--muted); }}
ol.recs {{ gap: 12px; font-size: 14.5px; }}
ol.recs a {{ color: var(--blue); text-decoration: none; font-size: 12px; font-weight: 500; }}
ul.note, ol.plain {{ margin: 0; padding-left: 18px; display: flex; flex-direction: column; gap: 4px; }}
details.code {{ border-top: 1px solid var(--faint); }}
details.code summary {{ cursor: pointer; padding: 8px 0; font-size: 13px; color: var(--blue); }}
details.code pre {{ margin: 0 0 8px; padding: 12px 14px; overflow-x: auto; font: 12px/1.5 var(--mono); background: var(--code-bg); color: var(--fg); }}
.codes {{ display: flex; flex-direction: column; }}
code {{ font-family: var(--mono); font-size: .9em; }}
blockquote.ask {{ margin: 0; border-left: 2px solid var(--rule); padding: 2px 0 2px 16px; display: flex; flex-direction: column; gap: 8px; }}
blockquote.ask ol {{ margin: 0; padding-left: 18px; }}
blockquote.ask footer {{ font-size: 12.5px; color: var(--muted); }}
dl.framing {{ display: grid; grid-template-columns: minmax(120px, 190px) 1fr; gap: 12px 24px; margin: 0; font-size: 14px; }}
dl.framing dt {{ font-weight: 600; }}
dl.framing dd {{ margin: 0; display: flex; flex-direction: column; gap: 6px; min-width: 0; }}
@media (max-width: 560px) {{ dl.framing {{ grid-template-columns: 1fr; gap: 2px; }} dl.framing dd {{ margin-bottom: 12px; }} }}
</style>
<main>
<header>
  <span class="eyebrow">DATA-2809 {SEP} Win pricing and packaging input {SEP} internal</span>
  <h1>Does race size change how Pro users use Win?</h1>
  <p class="meta">Paying Win Pro customers with a 2026 election (Pro today, or canceled after their election) {SEP} {N} people {SEP} six months up to and including election day {SEP}
  run {esc(RUN)} UTC {SEP} staff, demo and comped Pro excluded</p>
  <p class="meta status"><b>Status: reviewed draft.</b> Rebuilt 2026-11-12 on the marts with the revised population; results reviewed by Tristan; independent methodology and product reviews done and applied 2026-11-12. Owner: Tristan (product analytics).</p>
</header>

<section id="tldr">
  <span class="eyebrow">TL;DR</span>
  <p class="answer">No feature is used more by bigger races, so race size is no basis for feature packaging. What race size changes is texting:
  bigger races send more texts, though fewer per vote needed. Among all paying Pro customers in races needing 2,500 or more votes,
  {big.over_bundle.mean():.0%} sent more than 5,000 texts ({big_act.over_bundle.mean():.0%} of activated ones). And {int((~ws.texted).sum())} of {N} never sent a text,
  so a text bundle priced into the fee charges most customers for texts they do not send today.</p>
  <ul class="support">
    <li>No feature area rises with race size. On races from July 15 (when feature tracking works), the 10,000+ band is lower on every area, for example outreach {out_fm["10,000+"]:.0%} against {min(out_fm[b] for b in BANDS[:3]):.0%} to {max(out_fm[b] for b in BANDS[:3]):.0%}. Among July-on signups the bands are flat: opponent research {", ".join(f"{opp_after[b]:.0%}" for b in BANDS)}. <a href="#features">Section 2: features</a></li>
    <li>Bigger races send more texts (median {mt_small:,.0f} to {mt_big:,.0f} among texters) but fewer per vote needed (about {tv_small:.0f}, from only {int(kt[kt.band == "under 1,000"].shape[0])} texters, down to {tv_big:.1f}). <a href="#volume">Section 4: volume and spend</a></li>
    <li>Over 5,000 texts, all Pro customers: {", ".join(f"{over_up[b][0]} of {over_up[b][2]}" for b in BANDS)} from smallest to largest band. Counting every texter with no known count as over gives an upper bound of {", ".join(str(over_up[b][1]) for b in BANDS)}. <a href="#volume">Section 4: volume and spend</a></li>
    <li>{int(ws.texted.sum())} of {N} ({ws.texted.mean():.0%}) sent a text; {int(ws.tried_text_only.sum())} more started one and never sent it. {int(ws.no_outreach.sum())} ({ws.no_outreach.mean():.0%}) did no outreach on any channel, and {no_out_big.mean():.0%} in the 10,000+ band. <a href="#who-texts">Section 4: who texts</a></li>
    <li>Median outreach spend per payer rises from ${small_med:,.0f} to ${big_med:,.0f}. Texts cost about {per_text_lo * 100:.1f} to {per_text * 100:.1f} cents each, so 5,000 texts are about {cost_5k}. <a href="#volume">Section 4: volume and spend</a></li>
    <li>Phone banking is used about evenly across sizes; robocalls and door knocking peak in the middle bands. Door knocking is free today and {int(ws.used_door_knocking.sum())} of {N} used it. <a href="#channels">Section 3: channels</a></li>
    <li>Volume and spend hold without the early races, the races not yet held, or the people who canceled after their election. One statewide race is {top / tot_usd:.0%} of all outreach dollars, so the report uses medians. <a href="#robustness">Section 5: robustness</a></li>
  </ul>
  <p class="note terms"><b>Terms.</b> <b>Activated</b>, the company-wide definition (the activation OKR): reached voters at least once, on any channel, at any time, counting outreach they logged in Win themselves ({int(ws.activated.sum() - (ws.activated & (ws.sends_total > 0)).sum())} activated people had no recorded send in the window),
  so every activated user did some outreach ({len(act)} of {N}). <b>Texter</b>: sent at least one text in the window. <b>Votes needed</b>: half the projected turnout, plus one.
  <b>Window</b>: the six months before the election, election day included.</p>
  <nav class="toc" aria-label="Sections"><a href="#recommendations">Recommendations</a><a href="#votes-needed">1 Votes needed</a><a href="#features">2 Features</a><a href="#channels">3 Channels</a><a href="#volume">4 Volume and spend</a><a href="#robustness">5 Robustness</a><a href="#rerun">Rerun</a><a href="#appendix">Appendix: question and framing</a><a href="#assumptions">Assumptions</a></nav>
</section>

<section id="recommendations">
  <span class="eyebrow">Recommendations</span>
  <h2>What should product do with this?</h2>
  <p class="takeaway">The analyst's read for the pricing work, revised after the methodology and product reviews. Each item names the evidence behind it.</p>
  <ol class="plain recs">
    <li><b>Do not package features by race size.</b> No feature area is used more in bigger races. The largest races use every area less,
    mostly among people who signed up before July, so tiers built on "bigger races get opponent research" would charge for something big races use no more. <a href="#features">Section 2</a></li>
    <li><b>If outreach is bundled, size the text allowance by race, and price it against what people spend today.</b> Among texters, 75% are covered by
    {tex75["1,000-2,499"]} texts at 1,000 to 2,499 votes, {tex75["2,500-9,999"]} at 2,500 to 9,999 and {tex75["10,000+"]} at 10,000+. Across all Pro customers,
    5,000 texts already covers three in four of every band except 2,500 to 9,999 ({all75["2,500-9,999"]}). Texts per vote needed fall as races grow, so the allowance should grow
    less than in proportion: a base allowance with paid top-ups fits the spread better than one fixed number. 5,000 texts cost about {cost_5k},
    while the median payer spends ${med_pay["under 1,000"]:,.0f} under 1,000 votes and ${med_pay["1,000-2,499"]:,.0f} at 1,000 to 2,499, so for small races
    a $249 bundle charges for texts most would not send. People paid per text here; inside a bundle extra texts are free, so expect them to send more. <a href="#allowance">Section 4</a></li>
    <li><b>Treat the people who do not text as their own question.</b> {int((~ws.texted).sum())} of {N} paying Pro customers never sent a text, and {int(ws.no_outreach.sum())} did no outreach at all,
    {no_out_big.mean():.0%} in the 10,000+ band. {int((ws.tried_text_only & (ws.p2p_pending_payment > 0)).sum())} text campaigns stopped at payment, a small sign of price friction.
    A bundle raises their price for something they do not use; whether that is a pricing problem or an onboarding problem is not answered here. <a href="#who-texts">Section 4</a></li>
    <li><b>Look beyond race size for other ways to segment.</b> Race size explains text volume and little else. DATA-2810 tests other
    cuts (office level, timing of the upgrade, incumbency, usage groups) for differences in use and willingness to pay.</li>
  </ol>
</section>

<section id="votes-needed">
  <span class="eyebrow">1 {SEP} Input</span>
  <h2>Can we trust the votes-needed number?</h2>
  <p class="takeaway">Yes, for {known} of {N} users. gp-api stores no win number; it computes half the projected turnout plus one,
  from the override district, else the race picked in onboarding, else the organization's position. We repeat that rule.</p>
  <div class="grid2">
    <div><h3>Where each number came from</h3>{cov_tbl}</div>
    <div><h3>The bands</h3>{bs_tbl}</div>
  </div>
  <ul class="note"><li>{esc(A.sanity(ws))}.</li></ul>
  <div class="codes">
    {code("SQL: population and votes-needed rule", pop_sql)}
    {code("Python: choosing the source and assigning bands (analyze.load)", src(A.load), "py")}
  </div>
</section>

<section id="features">
  <span class="eyebrow">2 {SEP} Features</span>
  <h2>Do bigger races use different features?</h2>
  <p class="takeaway">No area is used more by bigger races. Among people who signed up from July the bands sit on the everyone line.
  Among earlier signups the 10,000+ band is lower on every area, and {(fm[fm.band == "10,000+"].signup_era == "before Jul 2026").mean():.0%} of that band signed up before July.
  So big races are less engaged across the board, not drawn to a particular feature. Every chart shares one layout: read straight down to compare.</p>
  <p class="note">Races from July 15 only ({len(fm)} of {N}). Before then the feature events record no actions in outreach, story or opponent research
  (older event names), though {int(ws[ws.early_election].texted.sum())} of those {n_early} people texted. Counting them would make the small band, where they are
  {int((ws.early_election & (ws.band == "under 1,000")).sum())} of {int((ws.band == "under 1,000").sum())} people, look less active than it is. Volume and spend below keep everyone.</p>
  <h3 class="group">All Pro users</h3>
  <div class="sub"><h3>All Pro users {SEP} {len(fm)}</h3>{feat_all}</div>
  <div class="sub"><h3>Signed up before July 2026 {SEP} {len(fm_before)}</h3>{feat_before}</div>
  <div class="sub"><h3>Signed up from July 2026 {SEP} {len(fm_after)}</h3>{feat_after}</div>
  <h3 class="group">Activated users only</h3>
  <p class="note">Activated, the company-wide definition: reached voters at least once, on any channel, counting outreach they logged themselves. {len(fm_act)} of {len(fm)}.</p>
  <div class="sub"><h3>Activated {SEP} {len(fm_act)}</h3>{feat_act}</div>
  <div class="sub"><h3>Activated, signed up before July 2026 {SEP} {len(fm_act_before)}</h3>{feat_act_before}</div>
  <div class="sub"><h3>Activated, signed up from July 2026 {SEP} {len(fm_act_after)}</h3>{feat_act_after}</div>
  <div class="sub"><h3>How deep: median action-days among users who acted</h3>{dep_tbl}</div>
  <div class="codes">
    {code("SQL: action-days per area (DATA-2690 area mapping)", asql)}
    {code("Python: rates and the within-era check", src(A.rate) + chr(10) + src(A.within), "py")}
  </div>
</section>

<section id="channels">
  <span class="eyebrow">3 {SEP} Channels</span>
  <h2>Do bigger races use different outreach channels?</h2>
  <p class="takeaway">Texting is the main channel at every size; among activated users it falls off only above 10,000 votes.
  Phone banking is about even across sizes. Robocalls and door knocking are mostly a middle-band habit. Door knocking is free today and {int(ws.used_door_knocking.sum())} of {N} used it, so "unlimited" in a bundle costs nothing and changes little. Only {un:,} of {tot:,} sends have an unknown channel.</p>
  <div class="sub"><h3>All Pro users {SEP} {N}</h3>{chan_fig}</div>
  <div class="sub"><h3>Activated users {SEP} {len(act)}</h3>{chan_fig_act}</div>
  <div class="codes">{code("SQL: sends per channel", sends_sql)}</div>
</section>

<section id="volume">
  <span class="eyebrow">4 {SEP} Volume and spend</span>
  <h2>Do bigger races send and spend more?</h2>
  <p class="takeaway">Fewer than half of Pro customers text at all ({int(ws.texted.sum())} of {N}; {int((~ws.texted).sum())} never did). Those who do send more texts in bigger races but far fewer per vote needed,
  and that holds among activated users and within each signup era.
  Texting is the only paid channel. Spend follows the Revenue dashboard's rule: outreach purchases minus refunds, on each user's main Stripe customer record.</p>
  <div class="sub" id="who-texts"><h3>Who texts at all (all Pro users)</h3>{who_tbl}
  <p class="note">Started a text, never sent: a text campaign created in the window but never sent, {int((ws.tried_text_only & (ws.p2p_pending_payment > 0)).sum())} of them stopped at payment.
  Other channels only: phone banking, door knocking, robocalls or social, no texts.</p>
  <h3>Activated is not the same as texted</h3>{act_tex_tbl}
  <p class="note">{gap_n} people are activated but sent no text in the window: {gap_other} reached voters on another channel in the window
  (mostly phone banking, door knocking, or sends with no recorded channel), and {gap_n - gap_other} activated outside the window or only logged their own outreach.
  {tex_not_act} texted without counting as activated (their sends don't meet the activation rules). {gap_unk} of the {gap_n} have sends with no recorded channel,
  some probably older texts, so "never sent a text" is an upper bound.</p></div>
  <div class="sub"><h3>How many texts</h3>{vol_fig}</div>
  <div class="sub"><h3>Who outgrows a 5,000-text bundle</h3>{over_fig}
  <p class="note">Upper bound, counting every texter with no known count as over: {", ".join(f"{over_up[b][1]} of {over_up[b][2]}" for b in BANDS)} from smallest to largest band.
  The gap is widest under 1,000 votes, where many texters (mostly spring races) have no count. Their known-count neighbors sent a median of {mt_small:,.0f}, so most of them are probably under.</p></div>
  <div class="sub" id="allowance"><h3>What allowance would cover most people</h3>{allow_tbl}
  <p class="note">Texts that cover 50%, 75% and 90% of each band. "All Pro" counts non-texters at zero, so its median is 0 everywhere. Texters with no known count are left out,
  which makes the cover points lean low. Under 30 texters with a known count: no texter figures. These are texts sent at about 3 cents each; inside a bundle people would likely send more.</p></div>
  <div class="sub"><h3>Who pays for outreach</h3>{paid_fig}</div>
  <div class="sub"><h3>How much they pay</h3>{spend_tbl}
  <p class="note">One statewide race spent ${top:,.0f}, {top / tot_usd:.0%} of the ${tot_usd:,.0f} total; ${tot_usd - top:,.0f} without it.
  Read the medians. Across all payers: 25th percentile ${q[0.25]:,.0f}, median ${q[0.5]:,.0f}, 75th ${q[0.75]:,.0f}, 90th ${q[0.9]:,.0f}.</p></div>
  <div class="codes">
    {code("SQL: outreach spend, the Revenue dashboard rule", money_sql)}
    {code("Python: volume and spend table (analyze.volume_table)", src(A.volume_table), "py")}
  </div>
</section>

<section id="robustness">
  <span class="eyebrow">5 {SEP} Robustness</span>
  <h2>Does it hold?</h2>
  <p class="takeaway">For volume and spend, yes: dropping the {n_early} races before July 15 or the {n_future} not yet held leaves the medians unchanged.
  Features are already measured on races from July 15 only (Section 2). The 10,000+ band mixes city races with federal ones (see below), so treat it as "big races" and no finer.</p>
  <div class="sub"><h3>The claimed differences, with 95% intervals</h3>{ci_tbl}
  <p class="note">Rate {SEP} Wilson 95% interval {SEP} count. Under 30 people: count only.</p></div>
  <div class="grid2">
    <div><h3>Without the {n_early} races before July 15</h3>{rob_early}</div>
    <div><h3>Without the {n_future} races after the run date</h3>{rob_future}</div>
  </div>
  <div class="sub"><h3>Pro today vs canceled after the election</h3>{status_tbl}
  <p class="note">People who canceled after their election use less, but {int(canc.early_election.sum())} of {len(canc)} had spring races, before the AI assistant
  and opponent research existed, so this is not an independent check of the feature findings. They pull rates down most in the under-1,000 band, where they are
  {int((canc.band == "under 1,000").sum())} of {int((ws.band == "under 1,000").sum())} people, but the pattern across bands is the same with Pro today only:
  over 5,000 texts {", ".join(f"{(pt := ws[(ws.pro_status == 'Pro today') & (ws.band == b)]).over_bundle.mean():.0%}" for b in BANDS)} from smallest to largest band.</p></div>
  <div class="sub"><h3>The 10,000+ band by office level</h3>{lvl_tbl}</div>
</section>

<section id="rerun">
  <span class="eyebrow">Rerun</span>
  <h2>How do I reproduce this?</h2>
  <ol class="plain note">
    <li>From <code>analytics/</code> with Databricks auth set up: <code>uv run --with pyarrow python projects/win_pro_votes_needed/build_working_set.py</code> (two queries; per-user parquet in <code>datasets/</code>, gitignored).</li>
    <li><code>uv run --with pyarrow python projects/win_pro_votes_needed/analyze.py</code> prints every number on this page.</li>
    <li><code>build_notebook.py</code> refreshes the notebook; <code>build_page.py out.html</code> rebuilds this page. <code>README.md</code> and the brief hold the definitions.</li>
  </ol>
  <p class="note">Numbers move between runs as candidacies and turnout projections update. Quote the run time beside any number.</p>
</section>

<section id="appendix">
  <span class="eyebrow">Appendix</span>
  <h2>What was asked, and how we framed it</h2>
  <h3>The question as asked</h3>
  <blockquote class="ask">
    <p>Product is iterating on pricing and packaging for Win and wants to know more about existing Win Pro users:</p>
    <ol><li>Does votes needed to win change which features people use? (e.g. is opposition research more important in larger races?)</li>
    <li>Does votes needed to win change which outreach channels people use and how much they spend? (e.g. do larger races buy more texts relative to race size?)</li></ol>
    <p>Why it matters: sales is proposing bundling outreach cost into the fee, e.g. $249 including 5K texts and unlimited door knocking instead of $99 per campaign. The answer tells us whether one bundle fits everyone or whether it should scale with race size.</p>
    <footer>Bryan Levine, via DATA-2809, 2026-11-11</footer>
  </blockquote>
  <h3>Framing, from the approved brief</h3>
  <dl class="framing">{framing_html}</dl>
  <h3 id="assumptions">Assumptions</h3>
  <p class="note">Every decision that shaped these numbers, in the order it was made, including the corrections after the first readout.</p>
  {assump_tbl}
  <div class="codes">{code("Full brief: win_pro_votes_needed_brief.yaml", brief_text, "yaml")}</div>
</section>
</main>
"""
out = Path(sys.argv[1])
out.write_text(page)
print(f"wrote {out} ({len(page):,} bytes)")
