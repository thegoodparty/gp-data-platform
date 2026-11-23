"""Win engagement report, 2026 cycle: sections 1 to 6.

Reads datasets/ (build_working_set.py), prints every table and writes review/page_data.json for the
draft page. Runs no queries.

Run from analytics/:
    uv run --with pyarrow python projects/win_engagement_2026/analyze.py
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
D = HERE / "datasets"
PAGE_DATA = HERE / "review" / "page_data.json"
PAGE: dict = {}
SENDS = pd.DataFrame()

QUIET_DAYS = 14  # assumption, see the brief
RETURN_WINDOW_DAYS = 2  # a return counts what people did that day and the next 2
WEEK_BINS = [-1, 3, 7, 11, 15, 25, 38, 51, 10_000]
WEEK_LABELS = ["0-3", "4-7", "8-11", "12-15", "16-25", "26-38", "39-51", "52+"]
RETURN_TOUCH_DAYS = 7
SMALL = 100  # rates on fewer people are faded on the page
TINY = 30  # below this only counts are shown
FUNNEL_START = pd.Timestamp("2025-05-26")  # onboarding events are recorded from here

# A feature counts only while candidates could use it themselves. Content builder was removed on
# 2026-09-10; website publishing has been done by staff acting for the candidate since July 2026;
# AI help was untracked 2026-07-29 to 10-19, so 2026 AI help use is a floor.
FEATURE_UNTIL = {"content_builder": "2026-09-10", "website_publish": "2026-06-30"}
FEATURE_LABEL = {
    "content_builder": "content builder",
    "voter_file_download": "voter file download",
    "segment_build": "audience building",
    "ai_help": "AI help",
    "website_publish": "website publishing",
    "outreach_prep": "outreach drafting",
    "plan_task": "campaign plan tasks",
    "story_rewrite": "story rewrite",
    "opponent_lookup": "opponent research",
    "team_assign": "team assignments",
    "team_invite": "team invites",
    "contact_tracking": "contact tracking",
}
# Steps inside the send flow: building an audience is part of creating a campaign, so their reach
# follows the flow more than a choice of tool.
STEP_FEATURES = {"segment_build", "voter_file_download", "outreach_prep"}
# Pro gating in omni (code check 2026-11-20). Contact tracking is open in the API but reached only
# through the Pro contacts list. Drafting is open server side; the webapp sent free users to the
# upgrade page from 2026-08-28 and, behind a flag, lets them draft since 09-18. Plan tasks lock only
# the text and robocall tasks. AI help, website publishing, story rewrite and team assignments are open.
PRO_GATED: set[str] = {"opponent_lookup", "contact_tracking"}
PRO_PARTIAL: set[str] = {"outreach_prep", "plan_task"}
# Year comparison at the level of the job, per the standing comparison rule.
JOB = {
    "ai_help": "got AI help",
    "content_builder": "got AI help",
    "voter_file_download": "pulled voter data",
    "segment_build": "built an audience",
    "website_publish": "published a website",
}
# HubSpot meeting outcomes. Held only on an explicit completed label; the rest is unknown.
HELD_OUTCOMES = {"COMPLETED", "CAS - MEETING COMPLETED"}
NOT_HELD_OUTCOMES = {
    "SCHEDULED",
    "NO_SHOW",
    "CANCELED",
    "RESCHEDULED",
    "CAS - ONBOARDING NO SHOW",
    "SERVE - NO SHOW",
    "CAS - MEETING SCHEDULED",
    "MANUALLY RESCHEDULE",
}
CONTACT_CHANNELS = {"call", "meeting", "email", "sms", "conversation"}
REPLY_DATA_START = pd.Timestamp("2026-07-22")
AS_OF = pd.Timestamp("2026-11-16")
BOOT = 2000

STATE_TZ = {
    **dict.fromkeys(
        "CT DE DC FL GA IN KY ME MD MA MI NH NJ NY NC OH PA RI SC VT VA WV".split(), "America/New_York"
    ),
    **dict.fromkeys("AL AR IL IA KS LA MN MS MO NE ND OK SD TN TX WI".split(), "America/Chicago"),
    **dict.fromkeys("CO ID MT NM UT WY".split(), "America/Denver"),
    "AZ": "America/Phoenix",
    **dict.fromkeys("CA NV OR WA".split(), "America/Los_Angeles"),
    "AK": "America/Anchorage",
    "HI": "Pacific/Honolulu",
}

ASSUMPTIONS = [
    ("Cycle", "Set by the election date: 2025 = Sep 1 to Dec 31 2025, 2026 = Jan 1 to Nov 3 2026."),
    (
        "One candidacy per user",
        "Someone who ran in both cycles counts once, in their main candidacy's cycle. 47 fall-2025 candidates (16 of them sent) sit in 2026 because they also registered a 2026 race.",
    ),
    (
        "Activated",
        "Sent voter outreach (or had our ops team send it) in the election's calendar year, by election day. The governed flag is lifetime and counted 124 more 2026 users whose only sends were in 2025; they are not activated here. The window only narrows the governed flag. Sends for an early-January race made in December don't count.",
    ),
    (
        "Pro history",
        "Pro today, Pro before but lapsed (a payment or Pro start on record), or never Pro. Pro status today is measured after the race, and most send channels need Pro, so history is the fairer trait.",
    ),
    ("Staff and demo", "Excluded everywhere (internal flag, demo-only accounts, goodparty.org emails)."),
    ("Activity", "A day with any non-machine Win event. Voter Guide traffic is not Win activity."),
    ("Quiet spell", f"{QUIET_DAYS}+ days with no Win activity. Chosen without a benchmark."),
    (
        "What a return did",
        f"Campaign work (a tool from the feature map, or a send) on the return day or the {RETURN_WINDOW_DAYS} days after. Where a return goes is the product area of the first Win event in the window outside the dashboard and Pro banners: the dashboard is where Win opens. Returns per 100 people a week count people who reached voters and had signed up by that week; a return needs 14 quiet days first, which caps it in the busiest weeks. Win events carry no referrer or UTM, so whether a return came from a link (an email, a text) can't be seen.",
    ),
    (
        "Weeks before the election",
        "Counted from signup to the candidate's own election date, not the November general. Bins widen further out because signups thin out.",
    ),
    (
        "Touch before a return",
        f"A logged HubSpot touch in the {RETURN_TOUCH_DAYS} days before the return, not counting the return day. Compared with the {RETURN_TOUCH_DAYS} days before that, inside spells of 21+ days so both weeks are quiet.",
    ),
    ("Funnel base", f"Signups from {FUNNEL_START.date()}, when onboarding steps start being recorded."),
    ("Pro timing", "Days from Pro to first send uses only users with a known Pro start date (about half)."),
    (
        "Sends",
        "A send counts when gp-api records it as completed or in progress (the send model's 'committed' basis). 2025 text and robocall requests that ops fulfilled outside the product stay 'requested' and are shown apart.",
    ),
    (
        "Replies",
        "Recorded from 2026-07-22, text sends only. A STOP is stored as a reply too, so a real reply is one without an opt-out. Checked against stored reply messages (from Aug 16): 64,241 people sent a non-STOP message against 51,937 stamped real replies on the same campaigns, so the real rate is about 0.6% to 0.75% of recipients.",
    ),
    (
        "Targeted send",
        "Uses at least one of 11 demographic filters (age, gender, homeowner, education, children, marital status, ethnicity, veteran, business owner, income, language). Party, voter likelihood, ideology and geography don't count.",
    ),
    (
        "Power users",
        "Power user v2 with the bar measured among people who reached voters: took real actions on as many days as the top 15% of senders in the same cycle's finished races (13 days in 2026, 9 in 2025; the bar scales with days out). 103 of 580 in 2026 (18%: ties at the bar count, and the bar is set on all senders in finished races, not only this report's group). 2025 counts only Sep to Dec races.",
    ),
    (
        "Corroborated",
        "A BallotReady, TechSpeed or DDHQ record exists for the candidacy (DATA-2202). Not the same as gp-api's verified flag.",
    ),
    (
        "Viability",
        "Viability score from contest facts only (seats, opponents, office), so reading it after the race does not leak the outcome.",
    ),
    (
        "Feature windows",
        "Content builder counts until its removal on 2026-09-10; website publishing until June 2026 (staff publish since). Each feature's share counts only people who signed up before its cutoff. AI help was untracked 2026-07-29 to 10-19, so 2026 AI help is a floor.",
    ),
    (
        "Pro gating",
        "Pro only: opponent research, and contact tracking in practice (it runs through the Pro contacts list). Partly: outreach drafting (free users sent to upgrade from 2026-08-28; free drafts allowed behind a flag from 09-18) and plan tasks (text and robocall tasks only). Open: AI help, website publishing, story rewrite, team assignments, voter file download, audience building.",
    ),
    (
        "Year comparison",
        "Features compare by the job they do (AI help and content builder are both 'got AI help'); features new in 2026 are shown for 2026 only.",
    ),
    (
        "CaS or sales contact",
        "An estimate from HubSpot. Held = meeting logged COMPLETED or 'CAS - MEETING COMPLETED', or a connected call. Other outcomes are 'unknown outcome'. 2025 is left out: CaS outcomes were not logged then.",
    ),
    (
        "Channel",
        "Self-signup vs sales-created from acquisition_motion. In 2025, 42% of sales-created accounts were made in hours with 10+ such accounts (up to 29 an hour); self-signups never pass 8 an hour, so these are accounts staff created in batches.",
    ),
    (
        "Robocalls",
        "About 90% of robocalls in the last two days before Nov 3 were a research team experiment; they are kept in.",
    ),
]


def rate(num: float, den: float) -> str:
    """'51% (6,250)': the share and the people behind it. Counts only under TINY."""
    if not den:
        return "-"
    if den < TINY:
        return f"{int(num):,} of {int(den):,}"
    return f"{num / den:.0%} ({int(num):,})"


def load() -> dict[str, pd.DataFrame]:
    out = {p.stem: pd.read_parquet(p) for p in D.glob("*.parquet")}
    u = out["users"]
    for c in [c for c in u.columns if c.endswith("_at")] + ["election_date", "pro_since"]:
        u[c] = pd.to_datetime(u[c], utc=True).dt.tz_localize(None)
    u["channel"] = np.where(
        u["acquisition_motion"].fillna("").str.contains("sales", case=False), "sales-created", "self-signup"
    )
    ev = pd.read_csv(D / "early_voting_by_state.csv")
    ev["ev_group"] = np.where(
        ev["all_mail"],
        "all-mail",
        np.where(ev["in_person_early_voting"], "in-person early voting", "neither"),
    )
    u["ev_group"] = u["state"].map(ev.set_index("state")["ev_group"])
    u["viability_band"] = (
        pd.cut(u["viability_score"], [-1, 1, 2, 99], labels=["under 1", "1 to 2", "2 and up"])
        .astype(str)
        .replace("nan", "unknown")
    )
    # is_pro is today's status, after the race; most send channels need Pro, so history matters.
    paid = u["first_subscription_payment_at"].notna() | u["pro_since"].notna()
    u["pro_history"] = np.where(u["is_pro"], "Pro today", np.where(paid, "Pro before, lapsed", "never Pro"))

    # Activated in a cycle = the governed flag AND a send (or ops send) in the election's calendar
    # year, by election day. The governed flag is lifetime, so a 2025 send for an earlier race made
    # 2026 users "activated" with no 2026 send. It only narrows the flag: sends the flag leaves out
    # on purpose (in-progress door knocking, unconfirmed p2p) don't add anyone.
    s = out["sends"].copy()
    s["send_at"] = pd.to_datetime(s["send_at"], utc=True).dt.tz_localize(None)
    s = s[s["basis"].isin(["committed", "requested"])].merge(u[["user_id", "election_date"]], on="user_id")
    in_window = (s["send_at"].dt.year == s["election_date"].dt.year) & (
        s["send_at"] < s["election_date"] + pd.Timedelta(days=1)
    )
    out["sends_in_window"] = s[in_window]
    global SENDS
    SENDS = s[in_window]
    first = s[in_window].groupby("user_id")["send_at"].min()
    cyc = u["cycle"].notna()
    u["is_activated_lifetime"] = u["is_activated"]
    u.loc[cyc, "is_activated"] = u.loc[cyc, "is_activated"] & u.loc[cyc, "user_id"].isin(first.index)
    # A first send before the window moves to the first send inside it.
    early = cyc & u["is_activated"] & (u["activated_at"] < u["user_id"].map(first))
    u.loc[early, "activated_at"] = u.loc[early, "user_id"].map(first)
    u.loc[cyc & ~u["is_activated"], "activated_at"] = pd.NaT
    u.loc[cyc & ~u["is_activated"], "is_power_user"] = False
    u["sent_outside_window"] = cyc & ~u["is_activated"] & u["is_activated_lifetime"]
    out["users"] = u
    return out


def cuts(u: pd.DataFrame) -> dict[str, pd.DataFrame]:
    a = u[u["is_activated"] & u["cycle"].notna()]
    return {
        "activated": a,
        "ICP activated": a[a["is_win_icp"]],
        "corroborated activated": a[a["is_corroborated"]],
        "power users": a[a["is_power_user"]],
    }


def by_cut(u: pd.DataFrame, fn, cycles=(2026, 2025)) -> pd.DataFrame:
    rows = []
    for cy in cycles:
        for name, df in cuts(u[u["cycle"] == cy]).items():
            r = fn(df)
            r = r if isinstance(r, pd.DataFrame) else pd.DataFrame([r])
            rows.append(r.assign(cycle=cy, cut=name, n=len(df)))
    return pd.concat(rows, ignore_index=True)


def show(title: str, df: pd.DataFrame) -> None:
    print(f"\n### {title}\n{df.to_string(index=False)}")


def boot_ci(x: np.ndarray, stat=np.mean, seed: int = 7) -> tuple[float, float]:
    rng = np.random.default_rng(seed)
    s = [stat(rng.choice(x, len(x))) for _ in range(BOOT)]
    return float(np.percentile(s, 2.5)), float(np.percentile(s, 97.5))


# ---------- Section 1: when do candidates sign up ----------
def section1(u: pd.DataFrame) -> None:
    # 2026 only: 2025 ran a different marketing approach.
    s = u[
        (u["registered_at"] >= "2026-01-01") & (u["registered_at"] < AS_OF.to_period("M").to_timestamp())
    ].copy()
    s["month"] = s["registered_at"].dt.to_period("M")
    m = s.groupby(["month", "channel"]).size().unstack(fill_value=0)
    show("1a. Signups per month by channel, 2026", m.reset_index())

    # Activated within 30 days of signup, on people whose 30 days have passed.
    s["old_enough"] = s["registered_at"] <= AS_OF - pd.Timedelta(days=30)
    s["act30"] = s["activated_at"].notna() & ((s["activated_at"] - s["registered_at"]).dt.days <= 30)
    g = s.groupby("month")
    r = (
        pd.DataFrame(
            {
                "signups": g.size(),
                "old_enough": g["old_enough"].sum(),
                "activated_30d": s[s["old_enough"]].groupby("month")["act30"].sum(),
            }
        )
        .fillna(0)
        .astype(int)
    )
    r["rate"] = (r["activated_30d"] / r["old_enough"].replace(0, np.nan)).round(3)
    r["filling"] = r["old_enough"] < r["signups"]
    show("1b. Share of each month's signups who sent within 30 days", r.reset_index())
    oe = s[s["old_enough"]]
    by_ch = (
        oe.groupby(["month", "channel"])
        .agg(old=("user_id", "size"), act=("act30", "sum"))
        .unstack(fill_value=0)
    )
    PAGE["signups"] = [
        {
            "month": str(k),
            **{c: int(m.loc[k].get(c, 0)) for c in ("self-signup", "sales-created")},
            "old_enough": int(r.at[k, "old_enough"]),
            "act": int(r.at[k, "activated_30d"]),
            "filling": bool(r.at[k, "filling"]),
            **{
                f"{v}|{c}": int(by_ch.loc[k, (v, c)]) if k in by_ch.index and (v, c) in by_ch.columns else 0
                for v in ("old", "act")
                for c in ("self-signup", "sales-created")
            },
        }
        for k in m.index
    ]
    show("1b by channel. Reached voters within 30 days, by signup month and channel", by_ch.reset_index())

    bins = [-10_000, 29, 89, 179, 364, 10_000]
    labels = ["<30d", "30-89d", "90-179d", "180-364d", "365d+"]

    def lead(df):
        d = (df["election_date"] - df["registered_at"].dt.normalize()).dt.days
        c = pd.cut(d, bins, labels=labels).value_counts().reindex(labels)
        return {**{k: rate(v, len(df)) for k, v in c.items()}, "median_days": d.median()}

    t = by_cut(u, lead)
    show("1c. Days from signup to election, by cut and cycle", t)
    PAGE["lead"] = t[["cycle", "cut", "n", "median_days"]].to_dict("records")
    t2 = pd.concat(
        [
            by_cut(u[u["channel"] == ch], lead, cycles=(2026,)).assign(channel=ch)
            for ch in ("self-signup", "sales-created")
        ]
    )
    show("1d. Same, by channel (2026)", t2)
    PAGE["lead_channel"] = t2[["cut", "channel", "n", "median_days"]].to_dict("records")

    # Weeks from signup to the candidate's own election, binned: a median hides a flat spread.
    s26 = u[u["cycle"] == 2026].copy()
    s26["weeks"] = (s26["election_date"] - s26["registered_at"].dt.normalize()).dt.days // 7
    s26["bin"] = pd.cut(s26["weeks"], WEEK_BINS, labels=WEEK_LABELS)
    g = (
        s26.groupby(["bin", "channel"], observed=False)
        .agg(signups=("user_id", "size"), sent=("is_activated", "sum"))
        .reset_index()
    )
    show("1e. 2026 signups and people who reached voters, by weeks from signup to their election", g)
    PAGE["lead_weeks"] = [
        {"bin": str(r.bin), "channel": r.channel, "signups": int(r.signups), "sent": int(r.sent)}
        for r in g.itertuples()
    ]
    act = s26[s26["is_activated"]].assign(november=lambda d: d["election_date"].dt.month.eq(11))
    m = act.groupby(["november", "channel"])["weeks"].agg(["median", "size"]).reset_index()
    show(
        "1f. Median weeks from signup to election among people who reached voters, November races against the rest",
        m,
    )
    PAGE["lead_nov"] = [
        {"november": bool(r.november), "channel": r.channel, "median_weeks": float(r[3]), "n": int(r[4])}
        for r in m.itertuples()
    ]


# ---------- Section 2: getting started ----------
def section2(u: pd.DataFrame, fd: pd.DataFrame, o: pd.DataFrame) -> None:
    steps = [
        ("signed up", "registered_at"),
        ("logged in at least once", "first_activity_at"),
        ("started onboarding", "onboarding_started_at"),
        ("finished onboarding", "onboarding_completed_at"),
        ("sent voter outreach", "activated_at"),
    ]
    base = u[u["cycle"].notna() & (u["registered_at"] >= FUNNEL_START)]
    PAGE["funnel"] = {}
    rows = []
    for cy in (2026, 2025):
        for ch in ("all", "self-signup", "sales-created"):
            df = base[base["cycle"] == cy]
            df = df if ch == "all" else df[df["channel"] == ch]
            n = len(df)
            prev = None
            out = []
            for label, col in steps:
                # A later step implies the earlier ones even where an event is missing.
                later = [c for _, c in steps[steps.index((label, col)) :]]
                reached = (
                    df[later].notna().any(axis=1)
                    if col != "onboarding_completed_at"
                    else (df["is_onboarded"] | df["activated_at"].notna())
                )
                k = int(reached.sum())
                gap = (df[col] - df[prev]).dt.days.median() if prev else np.nan
                out.append(
                    {
                        "step": label,
                        "n": k,
                        "share": round(k / n, 3) if n else np.nan,
                        "median_days_from_prev": gap,
                    }
                )
                rows.append(
                    {
                        "cycle": cy,
                        "channel": ch,
                        "step": label,
                        "reached": rate(k, n),
                        "median_days_from_prev": gap,
                    }
                )
                prev = col
            PAGE["funnel"][f"{cy}|{ch}"] = {"base": n, "steps": out}
    show(
        f"2a. Steps reached, signups from {FUNNEL_START.date()} with a race in the cycle (share of signups)",
        pd.DataFrame(rows),
    )

    nl = base[base["first_activity_at"].isna()]
    started = base["onboarding_started_at"].notna()
    show(
        "2a note. Why 'logged in' and 'started onboarding' are under 100%",
        pd.DataFrame(
            [
                {
                    "what": "never logged in (no product activity recorded)",
                    "people": rate(len(nl), len(base)),
                },
                {
                    "what": "logged in, never started onboarding",
                    "people": rate(int((base["first_activity_at"].notna() & ~started).sum()), len(base)),
                },
                {
                    "what": "marked onboarded without a recorded start",
                    "people": rate(int((~started & base["is_onboarded"]).sum()), int((~started).sum())),
                },
            ]
        ),
    )

    # Days from Pro to first send, by the channel of that first send. Positive = Pro came first.
    a = u[(u["cycle"] == 2026) & u["is_activated"] & u["pro_since"].notna()].copy()
    a["days"] = (a["activated_at"].dt.normalize() - a["pro_since"].dt.normalize()).dt.days
    g = a.groupby("first_activation_channel")
    t = pd.DataFrame(
        {
            "n": g.size(),
            "median_days": g["days"].median(),
            "pro_first": g["days"].apply(lambda s: (s >= 0).sum()),
        }
    )
    t.loc["all"] = [len(a), a["days"].median(), int((a["days"] >= 0).sum())]
    t["pro_first"] = [rate(p, n) for p, n in zip(t["pro_first"], t["n"], strict=False)]
    t = t.sort_values("n", ascending=False)
    show(
        "2b. Days from Pro to first send, 2026 activated Pro users with a known Pro date, by first send channel",
        t.reset_index(),
    )
    PAGE["pro_timing"] = [
        {"channel": k, "n": int(r["n"]), "median_days": float(r["median_days"])} for k, r in t.iterrows()
    ]

    # Did people use a channel once it was there? Available = the first 2026 send on that channel;
    # eligible = 2026 senders whose election was at least 14 days after that.
    w = SENDS[SENDS["user_id"].isin(u.loc[(u["cycle"] == 2026) & u["is_activated"], "user_id"])]
    a26 = u[(u["cycle"] == 2026) & u["is_activated"]]
    rows = []
    for ch, d0 in w[w["channel"] != "unattributed"].groupby("channel")["send_at"].min().items():
        el = a26[a26["election_date"] >= d0.normalize() + pd.Timedelta(days=14)]
        k = int(el["user_id"].isin(w.loc[w["channel"] == ch, "user_id"]).sum())
        rows.append(
            {
                "channel": ch,
                "available_from": str(d0.date()),
                "eligible": len(el),
                "used": k,
                "share": rate(k, len(el)),
            }
        )
    t = pd.DataFrame(rows).sort_values("used", ascending=False)
    show(
        "2b'. 2026 senders who used each channel, of those whose election was 14+ days after it became available",
        t,
    )
    PAGE["channels"] = t.drop(columns="share").to_dict("records")
    PAGE["unattributed_share"] = round(float((w["channel"] == "unattributed").mean()), 3)

    # Furthest step toward a send, 2026 onboarded people who never sent.
    st = u[(u["cycle"] == 2026) & u["is_onboarded"] & ~u["is_activated"]]
    f = fd[fd["user_id"].isin(st["user_id"])].merge(
        st[["user_id", "onboarding_completed_at", "election_date"]], on="user_id"
    )
    f["activity_date"] = pd.to_datetime(f["activity_date"])
    f = f[
        (f["activity_date"] >= f["onboarding_completed_at"].dt.normalize().fillna(pd.Timestamp("1900-01-01")))
        & (f["activity_date"] < f["election_date"])
    ]
    oo = o[o["user_id"].isin(st["user_id"]) & ~o["is_test"]]
    level = pd.Series(0, index=st["user_id"])
    level[level.index.isin(f["user_id"])] = 1
    level[level.index.isin(f.loc[f["feature"].isin(["segment_build", "voter_file_download"]), "user_id"])] = 2
    level[level.index.isin(f.loc[f["feature"] == "outreach_prep", "user_id"])] = 3
    level[level.index.isin(oo["user_id"])] = 4
    level[level.index.isin(st.loc[st["sent_outside_window"], "user_id"])] = 5
    level = level.replace({3: 2})  # drafting without an audience is 1 person: one step with audiences
    names = [
        "used no other Win tool",
        "used a Win tool, but no audience or draft",
        "built an audience, pulled voter data or drafted",
        "",
        "created a campaign that didn't go out",
        "reached voters before 2026, not in 2026",
    ]
    c = level.value_counts().reindex([0, 1, 2, 4, 5], fill_value=0)
    names = [names[i] for i in c.index]
    out = pd.DataFrame({"furthest_step": names, "people": [rate(v, len(st)) for v in c]})
    show(f"2c. Furthest step toward a send, 2026 onboarded people who never sent (n={len(st)})", out)
    PAGE["stall"] = {
        "n": len(st),
        "rows": [{"step": nm, "n": int(v)} for nm, v in zip(names, c, strict=False)],
    }
    unsent = oo.groupby("status")["user_id"].nunique().sort_values(ascending=False)
    show("2c note. Status of the unsent sends (people)", unsent.rename("people").reset_index())


# ---------- Section 3: which features bring value ----------
def feature_use(u: pd.DataFrame, fd: pd.DataFrame) -> pd.DataFrame:
    """One row per user x feature: first use and days used before the election."""
    fd = fd.merge(u[["user_id", "election_date"]], on="user_id")
    fd["activity_date"] = pd.to_datetime(fd["activity_date"])
    until = fd["feature"].map(FEATURE_UNTIL).fillna("2100-01-01").pipe(pd.to_datetime)
    fd = fd[(fd["activity_date"] < fd["election_date"]) & (fd["activity_date"] <= until)]
    return (
        fd.groupby(["user_id", "feature"])
        .agg(first=("activity_date", "min"), days=("activity_date", "nunique"))
        .reset_index()
    )


def eligible(df: pd.DataFrame, feature: str) -> pd.DataFrame:
    """People who signed up while the feature was still in candidates' hands."""
    cut = FEATURE_UNTIL.get(feature)
    return df if cut is None else df[df["registered_at"] < pd.Timestamp(cut)]


def feature_table(df: pd.DataFrame, per: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feat in FEATURE_LABEL:
        el = eligible(df, feat)
        p = per[(per["feature"] == feat) & per["user_id"].isin(el["user_id"])]
        rows.append(
            {
                "feature": feat,
                "eligible": len(el),
                "users": len(p),
                "used": round(len(p) / len(el), 3) if len(el) else np.nan,
                "repeat_users": int((p["days"] >= 2).sum()),
                "repeat_2plus_days": round((p["days"] >= 2).mean(), 3) if len(p) else np.nan,
                "pro_only": feat in PRO_GATED,
                "pro_partial": feat in PRO_PARTIAL,
                "send_step": feat in STEP_FEATURES,
            }
        )
    return pd.DataFrame(rows).sort_values("used", ascending=False)


def section3(u: pd.DataFrame, per: pd.DataFrame) -> None:
    PAGE["features"] = {}
    for name, df in cuts(u[u["cycle"] == 2026]).items():
        t = feature_table(df, per)
        show(
            f"3a. Feature use before the election, 2026 {name} (n={len(df)})",
            t.assign(
                used=[rate(a, b) for a, b in zip(t["users"], t["eligible"], strict=False)],
                repeat_2plus_days=[rate(a, b) for a, b in zip(t["repeat_users"], t["users"], strict=False)],
            ),
        )
        PAGE["features"][name] = t.to_dict("records")

    a = u[u["is_activated"] & (u["cycle"] == 2026)]
    PAGE["pro_history_counts"] = a["pro_history"].value_counts().to_dict()
    cols = {}
    for val, df in a.groupby("pro_history"):
        t = feature_table(df, per)
        cols[f"{val} ({len(df)})"] = t.set_index("feature").apply(
            lambda r: rate(r["users"], r["eligible"]), axis=1
        )
    pf = pd.DataFrame(cols)
    pf["gating"] = np.where(
        pf.index.isin(PRO_GATED), "Pro only", np.where(pf.index.isin(PRO_PARTIAL), "partly Pro", "")
    )
    show("3b. 2026 activated, share using each feature, by Pro history", pf.reset_index())
    for col, label in (("channel", "channel"), ("office_level", "office level")):
        parts = [
            feature_table(df, per).assign(**{label: val}) for val, df in a.groupby(col) if len(df) >= TINY
        ]
        t = pd.concat(parts)
        t["cell"] = [rate(x, y) for x, y in zip(t["users"], t["eligible"], strict=False)]
        show(
            f"3c. 2026 activated, by {label}: share using each feature",
            t.pivot(index="feature", columns=label, values="cell").reset_index(),
        )

    # 2026 against 2025 at the level of the job.
    pj = per.assign(job=per["feature"].map(JOB)).dropna(subset=["job"])
    pj = pj.groupby(["user_id", "job"]).agg(days=("days", "sum")).reset_index()
    rows = []
    for cy in (2026, 2025):
        for name, df in cuts(u[u["cycle"] == cy]).items():
            p = pj[pj["user_id"].isin(df["user_id"])]
            for job, g in p.groupby("job"):
                rows.append(
                    {
                        "job": job,
                        "cut": name,
                        "cycle": cy,
                        "n": len(df),
                        "users": len(g),
                        "used": round(len(g) / len(df), 3),
                        "cell": rate(len(g), len(df)),
                    }
                )
    t = pd.DataFrame(rows)
    show(
        "3d. Jobs tracked in both years, by cut and cycle",
        t.pivot(index=["job", "cut"], columns="cycle", values="cell").reset_index(),
    )
    PAGE["jobs"] = t[t["cut"] == "activated"].drop(columns="cell").to_dict("records")


# ---------- Section 4: what power users look like ----------
def crossed_bar(u: pd.DataFrame, ad: pd.DataFrame, bar: pd.DataFrame) -> pd.Series:
    """Day each power user first met the bar for that day's distance from the election, having sent."""
    b = bar.set_index(["cycle", "days_out"])["action_day_bar"]
    pu = u[u["is_power_user"]].set_index("user_id")
    ad = ad.assign(activity_date=pd.to_datetime(ad["activity_date"])).sort_values(
        ["user_id", "activity_date"]
    )
    out = {}
    for uid, g in ad.groupby("user_id"):
        if uid not in pu.index:
            continue
        el = pu.at[uid, "election_date"]
        g = g[g["activity_date"] <= el]
        n = np.arange(1, len(g) + 1)
        sent = g["has_send"].cummax().to_numpy()
        out_days = (el - g["activity_date"]).dt.days.clip(0, 365).to_numpy()
        bars = np.array([b.get((el.year, int(d)), np.nan) for d in out_days])
        ok = sent & (n >= bars)
        out[uid] = g["activity_date"].to_numpy()[ok.argmax()] if ok.any() else el
    return pd.Series(out, name="crossed_at")


TRAITS = [
    ("pro_history", "Pro history", None),
    ("is_win_icp", "ICP", {True: "ICP", False: "not ICP"}),
    ("channel", "signup channel", None),
    ("office_level", "office level", None),
    ("voter_count_bucket", "district size", None),
    ("is_incumbent", "incumbency", {True: "incumbent", False: "challenger"}),
    ("is_uncontested", "contest", {True: "uncontested", False: "contested"}),
    ("viability_band", "viability score", None),
    ("is_corroborated", "corroborated", {True: "corroborated", False: "not corroborated"}),
]


def section4(
    u: pd.DataFrame, per: pd.DataFrame, ad: pd.DataFrame, bar: pd.DataFrame, touches: pd.DataFrame
) -> None:
    a = u[u["is_activated"] & u["cycle"].notna()].copy()
    a["group"] = np.where(a["is_power_user"], "power users", "other activated")
    a["lead_days"] = (a["election_date"] - a["registered_at"].dt.normalize()).dt.days

    def profile(df):
        def r(mask):
            mask = mask.astype("boolean")
            return rate(int(mask.sum()), int(mask.notna().sum()))

        return {
            "n": len(df),
            "sales_created": r(df["channel"] == "sales-created"),
            "icp": r(df["is_win_icp"]),
            "pro": r(df["is_pro"]),
            "uncontested": r(df["is_uncontested"]),
            "incumbent": r(df["is_incumbent"]),
            "corroborated": r(df["is_corroborated"]),
            "signup_days_before_median": df["lead_days"].median(),
            "action_days_median": df["power_user_action_days"].median(),
            "action_areas_median": df["power_user_action_areas"].median(),
        }

    rows = [{"cycle": cy, "group": gname, **profile(df)} for (cy, gname), df in a.groupby(["cycle", "group"])]
    rows += [
        {"cycle": cy, "group": "ICP activated", **profile(df)}
        for cy, df in a[a["is_win_icp"]].groupby("cycle")
    ]
    show(
        "4a. Power users against the rest of activated users",
        pd.DataFrame(rows).sort_values(["cycle", "group"]),
    )

    # Ratio: power-user rate in a group over the overall rate, traits outside the definition.
    PAGE["pu_ratio"] = {}
    for cy in (2026, 2025):
        ac = a[a["cycle"] == cy]
        overall = ac["is_power_user"].mean()
        rows = []
        for col, label, names in TRAITS:
            for val, df in ac.groupby(ac[col].astype(object).where(ac[col].notna(), "unknown")):
                k = int(df["is_power_user"].sum())
                rows.append(
                    {
                        "trait": label,
                        "group": names.get(val, val) if names else str(val),
                        "n": len(df),
                        "power_users": k,
                        "rate": round(k / len(df), 3),
                        "ratio": round(k / len(df) / overall, 2),
                    }
                )
        t = pd.DataFrame(rows)
        PAGE["pu_ratio"][str(cy)] = {
            "overall": round(overall, 3),
            "n": len(ac),
            "power_users": int(ac["is_power_user"].sum()),
            "rows": rows,
        }
        show(
            f"4b. {cy}: power-user rate in each group against all activated ({rate(int(ac['is_power_user'].sum()), len(ac))})",
            t.assign(rate=[rate(k, n) for k, n in zip(t["power_users"], t["n"], strict=False)]),
        )
        if cy == 2026:
            PAGE["pu_answer"] = {
                "overall": round(overall, 3),
                "n": len(ac),
                "power_users": int(ac["is_power_user"].sum()),
            }

    a26 = a[a["cycle"] == 2026]
    for col in ("office_level", "voter_count_bucket"):
        t = a26.groupby("group")[col].value_counts().unstack(0).fillna(0).astype(int)
        show(f"4c. 2026 mix by {col} (people)", t.reset_index())

    ft = pd.concat(
        [
            feature_table(df, per)
            .set_index("feature")[["users", "eligible"]]
            .apply(lambda r: rate(r["users"], r["eligible"]), axis=1)
            .rename(g)
            for g, df in a26.groupby("group")
        ],
        axis=1,
    )
    show("4d. 2026 share using each feature, power users against other activated", ft.reset_index())
    PAGE["pu_features"] = {g: feature_table(df, per).to_dict("records") for g, df in a26.groupby("group")}

    # Did CaS or sales reach people before they crossed the bar (power users) or before their
    # first send (both groups, the like-for-like comparison)? 2026 only: no 2025 outcomes.
    cross = crossed_bar(u, ad, bar)
    t = touches.copy()
    t["touch_at"] = pd.to_datetime(t["touch_at"], utc=True).dt.tz_localize(None)
    t = t[
        t["channel"].isin(CONTACT_CHANNELS)
        & (t["is_outbound_touch"].fillna(False) | (t["channel"] == "meeting"))
    ]
    mo = t["meeting_outcome"]
    t["held"] = ((t["channel"] == "meeting") & mo.isin(HELD_OUTCOMES)) | t["is_connected_call"].fillna(False)
    t["unknown"] = (t["channel"] == "meeting") & ~mo.isin(HELD_OUTCOMES | NOT_HELD_OUTCOMES)
    t["email"] = t["channel"].isin(["email", "sms", "conversation"])
    t["team"] = np.where(t["is_cas"], "CaS", np.where(t["primary_team_name"] == "Sales", "Sales", None))
    t = t[t["team"].notna()]

    def reach(df, before: pd.Series, label: str):
        tt = t.merge(before.rename("cut_at").rename_axis("user_id").reset_index(), on="user_id")
        tt = tt[tt["touch_at"] < tt["cut_at"]]
        out = []
        for team in ("CaS", "Sales"):
            x = tt[(tt["team"] == team) & tt["user_id"].isin(df["user_id"])]
            held = set(x.loc[x["held"], "user_id"])
            unk = set(x.loc[x["unknown"], "user_id"]) - held
            call = set(x.loc[~x["email"], "user_id"]) - held - unk
            em = set(x["user_id"]) - held - unk - call
            n = len(df)
            out.append(
                {
                    "group": label,
                    "team": team,
                    "n": n,
                    "held": rate(len(held), n),
                    "outcome_unknown": rate(len(unk), n),
                    "call_or_meeting_not_held": rate(len(call), n),
                    "email_or_text_only": rate(len(em), n),
                    "none": rate(n - len(held | unk | call | em), n),
                    "_held": len(held),
                    "_unk": len(unk),
                }
            )
        return out

    pu = a26[a26["is_power_user"]]
    oth = a26[~a26["is_power_user"]]
    act = a26.set_index("user_id")["activated_at"]
    rows = reach(pu, cross[cross.index.isin(pu["user_id"])], "power users, before crossing the bar")
    rows += reach(pu, act[act.index.isin(pu["user_id"])], "power users, before first send")
    rows += reach(oth, act[act.index.isin(oth["user_id"])], "other activated, before first send")
    for h in ("Pro today", "Pro before, lapsed", "never Pro"):
        sub, sub_o = pu[pu["pro_history"] == h], oth[oth["pro_history"] == h]
        rows += reach(sub, act[act.index.isin(sub["user_id"])], f"power users, {h}, before first send")
        rows += reach(
            sub_o, act[act.index.isin(sub_o["user_id"])], f"other activated, {h}, before first send"
        )
    t4 = pd.DataFrame(rows)
    # The question for the page: were people CaS reached before their first send more often power users?
    tt = t[(t["team"] == "CaS") & t["held"]].merge(
        act.rename("cut_at").rename_axis("user_id").reset_index(), on="user_id"
    )
    called = set(tt.loc[tt["touch_at"] < tt["cut_at"], "user_id"])
    overall = a26["is_power_user"].mean()
    cr = []
    for lbl, df in (
        ("completed CaS call before first send", a26[a26["user_id"].isin(called)]),
        ("no completed CaS call before first send", a26[~a26["user_id"].isin(called)]),
    ):
        k = int(df["is_power_user"].sum())
        cr.append({"group": lbl, "n": len(df), "power_users": k, "ratio": round(k / len(df) / overall, 2)})
    show("4e'. Power-user ratio by a completed CaS call before first send, 2026", pd.DataFrame(cr))
    PAGE["cas_ratio"] = cr
    PAGE["cas_pro_called"] = {
        "called_pro_today": rate(int(a26.loc[a26["user_id"].isin(called), "is_pro"].sum()), len(called)),
        "not_called_pro_today": rate(
            int(a26.loc[~a26["user_id"].isin(called), "is_pro"].sum()),
            int((~a26["user_id"].isin(called)).sum()),
        ),
    }
    show("4e. 2026: reached by each team (estimate from HubSpot)", t4.drop(columns=["_held", "_unk"]))
    PAGE["cas"] = t4.to_dict("records")
    lag = (cross - pu.set_index("user_id")["activated_at"].dt.normalize()).dt.days.dropna()
    show(
        "4f. Days from first send to crossing the bar, 2026 power users", lag.describe().round(1).to_frame().T
    )
    print(f"crossed on the first-send day: {rate(int((lag <= 0).sum()), len(lag))}")


# ---------- Section 5: coming back ----------
def daily_activity(hourly: pd.DataFrame, u: pd.DataFrame) -> pd.DataFrame:
    h = hourly.merge(u[["user_id", "state"]], on="user_id", how="left")
    h["hour_utc"] = pd.to_datetime(h["hour_utc"], utc=True)
    h["tz"] = h["state"].map(STATE_TZ).fillna("America/Chicago")
    parts = []
    for tz, g in h.groupby("tz"):
        loc = g["hour_utc"].dt.tz_convert(tz)
        parts.append(g.assign(local_date=loc.dt.tz_localize(None).dt.normalize()))
    return pd.concat(parts)


# Where a return goes after the dashboard, grouped the way a reader names it.
AREA = {
    "win_outreach_planning": "Outreach",
    "win_voter_outreach": "Outreach",
    "win_outreach_scheduling": "Outreach",
    "win_contacts": "Contacts",
    "win_candidate_profile": "Profile",
    "win_voter_data": "Voter data",
}
RETURN_PACE_BINS = [
    (26, 52, "26-52 weeks"),
    (12, 25, "12-25 weeks"),
    (8, 11, "8-11 weeks"),
    (4, 7, "4-7 weeks"),
    (0, 3, "0-3 weeks"),
]

WEEKS = [26, 20, 16, 12, 8, 6, 4, 3, 2, 1, 0, -1, -2, -4]


def weekly_curve(df: pd.DataFrame, w: pd.DataFrame, weeks=WEEKS) -> dict:
    """Share active each week out, of people signed up by that week and whose week has passed."""
    out = {}
    act = w[w["user_id"].isin(df["user_id"])]
    for k in weeks:
        start = df["election_date"] - pd.Timedelta(days=7 * k)
        # weeks_out k covers election - 7k - 6 .. election - 7k (week start, Monday-aligned in the mart).
        at_risk = df[(df["registered_at"] <= start) & (start + pd.Timedelta(days=7) <= AS_OF)]
        n = len(at_risk)
        hit = act[(act["weeks_out"] == k) & act["user_id"].isin(at_risk["user_id"])]["user_id"].nunique()
        out[k] = (hit, n)
    return out


def return_actions(
    ret: pd.DataFrame, fd: pd.DataFrame, ad: pd.DataFrame, first_events: pd.DataFrame, u: pd.DataFrame
) -> None:
    """2026 returns after a quiet spell: what people opened first, and what campaign work followed."""
    ret = ret[ret["cycle"] == 2026][["user_id", "local_date"]]
    f = fd.assign(activity_date=pd.to_datetime(fd["activity_date"]))[["user_id", "activity_date", "feature"]]
    a = ad.assign(activity_date=pd.to_datetime(ad["activity_date"]))
    work = pd.concat([f, a[a["has_send"]][["user_id", "activity_date"]].assign(feature="outreach_send")])
    m = ret.merge(work, on="user_id")
    m = m[(m["activity_date"] - m["local_date"]).dt.days.between(0, RETURN_WINDOW_DAYS)]
    did = m.groupby(["user_id", "local_date"])["feature"].apply(set)
    r = ret.set_index(["user_id", "local_date"]).join(did.rename("did"))
    n = len(r)
    looked = int(r["did"].isna().sum())
    counts = r["did"].dropna().explode().value_counts()
    rows = [{"feature": k, "returns": int(v)} for k, v in counts.items() if v >= TINY]
    show(
        f"5g. 2026 returns after {QUIET_DAYS}+ quiet days: campaign work that day or the next {RETURN_WINDOW_DAYS} (n={n})",
        pd.DataFrame([{"feature": "no campaign work", "returns": looked}, *rows]).assign(
            share=lambda d: (d["returns"] / n).round(3)
        ),
    )
    # Where people go first once past the dashboard (where Win opens), within the same window.
    fe = daily_activity(first_events, u)
    j = ret.merge(fe, on="user_id")
    j = j[(j["local_date_y"] - j["local_date_x"]).dt.days.between(0, RETURN_WINDOW_DAYS)].sort_values(
        "hour_utc"
    )
    went = j.groupby(["user_id", "local_date_x"])["product_area"].first().map(AREA).fillna("somewhere else")
    wc = went.value_counts()
    show(
        "5h. Where 2026 returns go first after the dashboard, within the window",
        wc.rename("returns").reset_index(),
    )

    # Returns per 100 people a week, by weeks before the election: the clock people come back on.
    who = u[u["is_activated"] & u["cycle"].eq(2026)][["user_id", "registered_at", "election_date"]]
    r = ret.merge(who, on="user_id")
    r["wk"] = (r["election_date"] - r["local_date"]).dt.days // 7
    pace = []
    for lo, hi, lab in RETURN_PACE_BINS:
        weeks = range(lo, hi + 1)
        # Person-weeks: people already signed up by each week, whose week has passed.
        pw = sum(
            int(((who["election_date"] - pd.to_timedelta(7 * w, "D")) > who["registered_at"]).sum())
            for w in weeks
        )
        k = int(r["wk"].between(lo, hi).sum())
        pace.append(
            {"bin": lab, "returns": k, "person_weeks": pw, "per100": round(100 * k / pw, 2) if pw else None}
        )
    show(
        "5i. 2026 returns after 14+ quiet days per 100 people a week, by weeks before the election",
        pd.DataFrame(pace),
    )
    PAGE["return_actions"] = {
        "n": n,
        "people": int(ret["user_id"].nunique()),
        "looked": looked,
        "rows": rows,
        "went": {k: int(v) for k, v in wc.items()},
        "went_n": int(len(went)),
        "pace": pace,
    }


def section5(
    u: pd.DataFrame,
    weekly: pd.DataFrame,
    hourly: pd.DataFrame,
    touches: pd.DataFrame,
    o: pd.DataFrame,
    fd: pd.DataFrame,
    ad: pd.DataFrame,
    first_events: pd.DataFrame,
) -> None:
    w = weekly.merge(u.loc[u["cycle"].notna(), ["user_id", "election_date"]], on="user_id")
    w["week_start_date"] = pd.to_datetime(w["week_start_date"])
    w["weeks_out"] = ((w["election_date"] - w["week_start_date"]).dt.days // 7).astype(int)

    PAGE["weekly"] = {}
    rows = []
    for cy in (2026, 2025):
        for name, df in cuts(u[u["cycle"] == cy]).items():
            c = weekly_curve(df, w)
            pts = []
            for k, (hit, n) in c.items():
                # 2026 after-election weeks wait until December so the late-November weeks are complete.
                blank = n < SMALL or (cy == 2026 and k < 0)
                pts.append({"w": k, "hit": hit, "n": n, "rate": None if blank else round(hit / n, 3)})
            rows.append(
                {
                    "cycle": cy,
                    "cut": name,
                    **{
                        f"w{p['w']}": ("-" if p["rate"] is None else f"{p['rate']:.0%} ({p['n']})")
                        for p in pts
                    },
                }
            )
            PAGE["weekly"][f"{cy}|{name}"] = pts
    for cy in (2026, 2025):
        df = u[(u["cycle"] == cy) & u["is_onboarded"] & ~u["is_activated"]]
        c = weekly_curve(df, w)
        pts = [
            {
                "w": k,
                "hit": h,
                "n": n,
                "rate": None if (n < SMALL or (cy == 2026 and k < 0)) else round(h / n, 3),
            }
            for k, (h, n) in c.items()
        ]
        PAGE["weekly"][f"{cy}|finished onboarding, never sent"] = pts
        rows.append(
            {
                "cycle": cy,
                "cut": "finished onboarding, never sent",
                **{f"w{p['w']}": ("-" if p["rate"] is None else f"{p['rate']:.0%} ({p['n']})") for p in pts},
            }
        )
    show(
        "5a. Share active each week before the election (negative = after), of people signed up by then; blank under 100 people and for 2026 after-election weeks",
        pd.DataFrame(rows),
    )

    def days_per_week(df):
        ww = w[w["user_id"].isin(df["user_id"]) & w["weeks_out"].between(0, 26)]
        return {
            "active_days_per_active_week": round(ww["win_activity_days"].mean(), 2),
            "active_weeks_median": ww.groupby("user_id").size().median(),
        }

    show("5b. Active days per active week, last 26 weeks before the election", by_cut(u, days_per_week))

    # Early voting: does activity peak earlier where people vote early?
    a26 = u[u["is_activated"] & (u["cycle"] == 2026)]
    PAGE["weekly_ev"] = {}
    rows = []
    for grp, df in a26.groupby("ev_group"):
        c = weekly_curve(df, w, weeks=list(range(12, -1, -1)))
        PAGE["weekly_ev"][grp] = [
            {"w": k, "hit": h, "n": n, "rate": round(h / n, 3) if n else None} for k, (h, n) in c.items()
        ]
        rows.append({"group": grp, "n": len(df), **{f"w{k}": rate(h, n) for k, (h, n) in c.items()}})
    show(
        "5c. 2026 activated: share active each week before the election, by the state's early voting",
        pd.DataFrame(rows),
    )

    # How far ahead sends are scheduled: created date to scheduled date, committed sends.
    # Native door knocking and phone banking start when created, so only scheduled channels count.
    s = o[
        (o["basis"] == "committed")
        & o["scheduled_date"].notna()
        & ~o["is_test"]
        & o["outreach_type"].isin(["p2p", "text", "robocall"])
    ].merge(u[["user_id", "cycle"]], on="user_id")
    s = s[s["cycle"].notna()]
    s["lead"] = (
        pd.to_datetime(s["scheduled_date"], utc=True).dt.tz_localize(None).dt.normalize()
        - pd.to_datetime(s["created_at"], utc=True).dt.tz_localize(None).dt.normalize()
    ).dt.days
    s = s[s["lead"] >= 0]
    bins, labels = [-1, 0, 2, 6, 13, 10_000], ["same day", "1-2 days", "3-6 days", "7-13 days", "14+ days"]
    rows = []
    PAGE["sched"] = []
    for (cy, ch), g in s.groupby(["cycle", "outreach_type"]):
        if len(g) < TINY:
            continue
        c = pd.cut(g["lead"], bins, labels=labels).value_counts().reindex(labels)
        rows.append(
            {
                "cycle": int(cy),
                "channel": ch,
                "sends": len(g),
                "median_days": g["lead"].median(),
                **{k: rate(v, len(g)) for k, v in c.items()},
            }
        )
        PAGE["sched"].append(
            {
                "cycle": int(cy),
                "channel": ch,
                "n": len(g),
                "median": float(g["lead"].median()),
                "bins": {k: int(v) for k, v in c.items()},
            }
        )
    show(
        "5d. Days from creating a send to its scheduled day, committed sends with a scheduled day",
        pd.DataFrame(rows),
    )

    # Quiet spells: a touch in the week before a return, against the week before that, inside the
    # same spell. Other active days are no baseline: the team talks more to people already active.
    d = daily_activity(hourly, u)
    # Activated in the cycle as this report counts it: the event pull uses the lifetime flag.
    d = d.merge(u.loc[u["is_activated"], ["user_id", "cycle", "election_date"]], on="user_id")
    d = d[d["local_date"] < d["election_date"]]
    days = d[["user_id", "local_date", "cycle"]].drop_duplicates().sort_values(["user_id", "local_date"])
    days["gap"] = days.groupby("user_id")["local_date"].diff().dt.days
    ret = days[days["gap"] >= QUIET_DAYS].copy()
    t = touches.copy()
    t["touch_at"] = pd.to_datetime(t["touch_at"], utc=True).dt.tz_localize(None)
    t = t[t["is_outbound_touch"].fillna(True)]
    m = ret.merge(t[["user_id", "touch_at", "is_cas"]], on="user_id")
    rel = (m["local_date"] - m["touch_at"].dt.normalize()).dt.days  # return-day touches (0) excluded
    m["week_before"] = rel.between(1, RETURN_TOUCH_DAYS)
    m["week_prior"] = rel.between(RETURN_TOUCH_DAYS + 1, 2 * RETURN_TOUCH_DAYS)
    hit = m.groupby(["user_id", "local_date"])[["week_before", "week_prior"]].any().reset_index()
    ret = ret.merge(hit, on=["user_id", "local_date"], how="left")
    ret[["week_before", "week_prior"]] = ret[["week_before", "week_prior"]].fillna(False).astype(bool)
    rows = []
    for cy, g in ret.groupby("cycle"):
        long = g[g["gap"] >= 3 * RETURN_TOUCH_DAYS]  # both weeks fall inside the quiet spell
        rows.append(
            {
                "cycle": int(cy),
                "returns": len(g),
                "no_touch_week_before": rate(int((~g["week_before"]).sum()), len(g)),
                "spells_21plus": len(long),
                "touch_week_before": rate(int(long["week_before"].sum()), len(long)),
                "touch_week_prior": rate(int(long["week_prior"].sum()), len(long)),
                "_no": int((~g["week_before"]).sum()),
                "_n": len(g),
                "_b": int(long["week_before"].sum()),
                "_p": int(long["week_prior"].sum()),
                "_l": len(long),
            }
        )
    PAGE["returns"] = rows
    return_actions(ret, fd, ad, first_events, u)
    show(
        f"5e. Returns after {QUIET_DAYS}+ quiet days: team touch in the week before (return day excluded); for 21+ day spells, against the week before that",
        pd.DataFrame(rows).drop(columns=["_no", "_n", "_b", "_p", "_l"]),
    )

    past = u[u["is_activated"] & u["cycle"].notna() & (u["election_date"] <= AS_OF - pd.Timedelta(weeks=4))]
    post = w[w["user_id"].isin(past["user_id"]) & w["weeks_out"].between(-4, -1)]
    pr = (
        past.assign(back=past["user_id"].isin(post["user_id"]))
        .groupby(["cycle", "is_win_icp"])["back"]
        .agg(["sum", "size"])
    )
    pr["active"] = [rate(s_, n) for s_, n in zip(pr["sum"], pr["size"], strict=False)]
    PAGE["after"] = [
        {"cycle": int(cy), "icp": bool(icp), "active": r["active"]} for (cy, icp), r in pr.iterrows()
    ]
    show(
        "5f. Active in the 4 weeks after the election (activated, races 4+ weeks past; provisional until Dec 1)",
        pr.reset_index(),
    )


# ---------- Section 6: outreach ----------
def send_volume(u: pd.DataFrame, sends: pd.DataFrame) -> None:
    """Senders and sends from the send model. 2025 texts were ops-fulfilled and stay 'requested', so
    the like-for-like year view counts committed or requested sends; 2026 also shows committed only."""
    s = sends.merge(u[["user_id", "cycle"]], on="user_id")  # already limited to the activation window
    s = s[s["cycle"].notna()]
    show(
        "6 basis. Send model rows by cycle, channel, basis and source",
        s.groupby(["cycle", "channel", "basis", "source"]).size().rename("sends").reset_index(),
    )

    def per(df):
        out = {}
        for nm, b in (("committed", {"committed"}), ("committed_or_requested", {"committed", "requested"})):
            x = s[s["user_id"].isin(df["user_id"]) & s["basis"].isin(b)]
            out[f"sends_per_sender_{nm}"] = x.groupby("user_id").size().median()
        return out

    t = by_cut(u, per)
    show("6a. Sends per activated sender (median), from the send model", t)
    PAGE["sends_per_sender"] = t.to_dict("records")


def section6(u: pd.DataFrame, o: pd.DataFrame) -> None:
    o = o[~o["is_test"]].merge(
        u[["user_id", "cycle", "is_activated", "is_win_icp", "is_corroborated", "is_power_user", "channel"]],
        on="user_id",
    )
    o["send_at"] = pd.to_datetime(o["send_at"], utc=True).dt.tz_localize(None)
    o = o[o["cycle"].notna() & o["send_at"].notna()]
    o["targeted"] = o["demographic_dimension_count"].fillna(0) > 0
    # Targeting is known only for gp-api sends with an audience: committed ones exist from 2026.
    c = o[o["basis"] == "committed"]

    def per_sender(df):
        oo = c[c["user_id"].isin(df["user_id"])]
        g = oo.groupby("user_id")
        return {
            "gp_api_senders": oo["user_id"].nunique(),
            "senders_targeting": rate(int(g["targeted"].any().sum()), oo["user_id"].nunique()),
            "sends_targeted": rate(int(oo["targeted"].sum()), len(oo)),
        }

    show("6b. Targeting, 2026 committed gp-api sends", by_cut(u, per_sender, cycles=(2026,)))
    r25 = o[(o["basis"] == "requested") & (o["cycle"] == 2025) & o["is_activated"]]
    print(
        f"2025 reference, ops-fulfilled text and robocall requests (activated): senders targeting "
        f"{rate(int(r25.groupby('user_id')['targeted'].any().sum()), r25['user_id'].nunique())}, "
        f"requests targeted {rate(int(r25['targeted'].sum()), len(r25))}"
    )
    a26 = c[(c["cycle"] == 2026) & c["is_activated"]]
    PAGE["targeting"] = {
        "senders": int(a26["user_id"].nunique()),
        "senders_targeting": int(a26.groupby("user_id")["targeted"].any().sum()),
        "sends": len(a26),
        "sends_targeted": int(a26["targeted"].sum()),
    }
    ch = (
        c.groupby(["cycle", "outreach_type"])
        .agg(sends=("outreach_id", "size"), senders=("user_id", "nunique"))
        .reset_index()
    )
    show("6b channels. 2026 committed gp-api sends by channel ('p2p' = in-product Peerly texting)", ch)
    pm = c[c["cycle"] == 2026].groupby("outreach_type")["purpose"].value_counts()
    show("6b purpose. Purpose mix by channel, 2026 committed sends", pm.rename("sends").reset_index())
    PAGE["purpose"] = [
        {"channel": ch_, "purpose": pu, "sends": int(v), "of": int(pm[ch_].sum())}
        for (ch_, pu), v in pm.items()
    ]

    # Replies, texts with recipient rows. Lead with each sender against themselves.
    x = c[(c["send_at"] >= REPLY_DATA_START) & (c["recipients"].fillna(0) > 0)].copy()
    x["reply"] = x["replied_real"] / x["recipients"]
    x["optout"] = x["opted_out"] / x["recipients"]
    both = x.groupby(["user_id", "targeted"])[["reply", "optout"]].mean().unstack().dropna()
    res = {"senders_with_both": len(both)}
    if len(both):
        for k in ("reply", "optout"):
            dlt = (both[(k, True)] - both[(k, False)]).to_numpy()
            lo, hi = boot_ci(dlt, np.median)
            res |= {
                f"{k}_diff_median": round(float(np.median(dlt)), 4),
                f"{k}_ci": f"[{lo:.4f}, {hi:.4f}]",
                f"{k}_untargeted_median": round(float(both[(k, False)].median()), 4),
            }
        PAGE["within_sender"] = {
            "n": len(both),
            "diffs": (both[("reply", True)] - both[("reply", False)]).round(5).tolist(),
            **res,
        }
    same = (
        x[x["user_id"].isin(both.index)]
        .groupby("targeted")[["recipients", "replied_real", "opted_out"]]
        .sum()
    )
    PAGE["within_pooled"] = {
        ("targeted" if k else "untargeted"): {
            "recipients": int(r["recipients"]),
            "replies_per_1000": round(1000 * r["replied_real"] / r["recipients"], 2),
            "optouts_per_1000": round(1000 * r["opted_out"] / r["recipients"], 1),
        }
        for k, r in same.iterrows()
    }
    show(
        "6c'. Same senders, pooled: real replies and opt-outs per 1,000 texts",
        pd.DataFrame(PAGE["within_pooled"]).T.reset_index(),
    )
    show(
        f"6c. Within sender: targeted minus untargeted text sends since {REPLY_DATA_START.date()} (real replies, bootstrap 95% CI on the median)",
        pd.DataFrame([res]),
    )
    agg = x.groupby("targeted").agg(
        sends=("outreach_id", "size"),
        senders=("user_id", "nunique"),
        recipients=("recipients", "sum"),
        real_replies=("replied_real", "sum"),
        opted_out=("opted_out", "sum"),
        replied_incl_stop=("replied", "sum"),
    )
    agg["real_reply_rate"] = (agg["real_replies"] / agg["recipients"]).round(4)
    agg["optout_rate"] = (agg["opted_out"] / agg["recipients"]).round(4)
    PAGE["pooled"] = agg.reset_index().to_dict("records")
    show("6d. Appendix: pooled reply and opt-out per recipient (mixes senders)", agg.reset_index())
    tot = x[["recipients", "replied", "opted_out", "replied_real"]].sum()
    PAGE["replies"] = {k: int(v) for k, v in tot.items()}
    print(
        f"real reply rate {tot['replied_real'] / tot['recipients']:.2%}; with STOPs {tot['replied'] / tot['recipients']:.2%}"
    )


def main() -> None:
    ds = load()
    u = ds["users"]
    print("run_ts:", (D / "run_ts.txt").read_text())
    show("Assumptions", pd.DataFrame(ASSUMPTIONS, columns=["assumption", "detail"]))
    pop = (
        u[u["cycle"].notna()]
        .groupby("cycle")
        .agg(
            users=("user_id", "size"),
            activated=("is_activated", "sum"),
            icp_activated=("is_win_icp", lambda s: (s & u.loc[s.index, "is_activated"]).sum()),
            corroborated_activated=("is_corroborated", lambda s: (s & u.loc[s.index, "is_activated"]).sum()),
            power_users=("is_power_user", "sum"),
        )
        .reset_index()
    )
    show("Population", pop)
    PAGE["population"] = pop.astype(int).to_dict("records")
    PAGE["assumptions"] = ASSUMPTIONS
    PAGE["run_ts"] = (D / "run_ts.txt").read_text()
    o = ds["outreach"]
    section1(u)
    per = feature_use(u, ds["feature_days"])
    section2(u, ds["feature_days"], o)
    section3(u, per)
    section4(u, per, ds["action_days"], ds["power_bar"], ds["touches"])
    section5(
        u,
        ds["weekly"],
        ds["hourly"],
        ds["touches"],
        o,
        ds["feature_days"],
        ds["action_days"],
        ds["first_events"],
    )
    send_volume(u, ds["sends_in_window"])
    section6(u, o)
    data = json.dumps(PAGE, default=lambda v: None if pd.isna(v) else str(v))
    PAGE_DATA.write_text(data)
    # The draft page is the template with this run's numbers inlined (NaN is valid JS).
    tpl = (HERE / "page_template.html").read_text()
    (HERE / "review" / "win_engagement_2026_draft.html").write_text(tpl.replace("/*DATA*/", data))


if __name__ == "__main__":
    main()
