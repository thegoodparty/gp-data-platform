"""Usage archetypes for Win (DATA-2690): rule labels, mix clusters, group profiles, journeys.

Reads the datasets written by build_working_set.py; no warehouse queries. Spec:
usage_archetypes_brief.yaml. Writes results.txt and results.json beside this file.

Run from analytics/:
    uv run --with scikit-learn --with pyarrow python projects/win_usage_archetypes/analyze.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score, silhouette_score

HERE = Path(__file__).resolve().parent
DATA = HERE / "datasets"
AREAS = ["outreach", "voter_data", "ai", "story", "opponents"]
AREA_NAMES = {
    "outreach": "Outreach",
    "voter_data": "Voter data",
    "ai": "AI assistant",
    "story": "Story and issues",
    "opponents": "Opponent research",
}
MIN_AREA_DAYS = 3  # fewer than this and there is no mix to cluster
K_RANGE = range(3, 9)
SEED = 7
OUT_LINES: list[str] = []
RESULTS: dict = {}


def say(*parts: object) -> None:
    line = " ".join(str(p) for p in parts)
    print(line)
    OUT_LINES.append(line)


def pct(x: float) -> str:
    return "n/a" if pd.isna(x) else f"{100 * x:.0f}%"


def load() -> tuple[pd.DataFrame, pd.DataFrame]:
    ws = pd.read_parquet(DATA / "working_set.parquet")
    for c in ws.columns:
        if ws[c].dtype == object and c not in ("signup_month", "office_level", "run_ts", "election_date"):
            ws[c] = pd.to_numeric(ws[c], errors="coerce")
    ad = pd.read_parquet(DATA / "area_days.parquet")
    # Mix and journeys use action-days (did something); visit-days (any page view) are kept as
    # view_* for the sensitivity check, since outreach and voter data are mostly visits.
    ad["action_days"] = pd.to_numeric(ad["action_days"])
    days = ad.pivot_table(index="user_id", columns="area", values="action_days", aggfunc="sum").reindex(
        columns=AREAS
    )
    views = ad.pivot_table(index="user_id", columns="area", values="days", aggfunc="sum").reindex(
        columns=AREAS
    )
    first = ad.pivot_table(index="user_id", columns="area", values="first_action_day", aggfunc="min").reindex(
        columns=AREAS
    )
    ws = ws.set_index("user_id")
    for a in AREAS:
        ws[f"days_{a}"] = days[a].reindex(ws.index).fillna(0)
        ws[f"view_{a}"] = views[a].reindex(ws.index).fillna(0)
        ws[f"first_{a}"] = pd.to_datetime(first[a].reindex(ws.index))
    ws["area_days"] = ws[[f"days_{a}" for a in AREAS]].sum(axis=1)
    ws["areas_used"] = (ws[[f"days_{a}" for a in AREAS]] > 0).sum(axis=1)
    for a in AREAS:
        ws[f"share_{a}"] = np.where(
            ws["area_days"] > 0, ws[f"days_{a}"] / ws["area_days"].where(ws["area_days"] > 0), 0
        )
    ws["weeks"] = ws["days_on_win"].clip(lower=14) / 7
    ws["area_days_per_week"] = ws["area_days"] / ws["weeks"]
    ws["experienced"] = ws["has_run_before"].fillna(False).astype(bool) | (
        ws["election_year_count"].fillna(1) > 1
    )
    ws["is_pro"] = ws["is_pro"].fillna(False).astype(bool)
    ws["activated"] = ws["activated"].fillna(False).astype(bool)
    ws["onboarded"] = ws["onboarded"].fillna(0).astype(int).astype(bool)

    tp = pd.read_parquet(DATA / "chat_topics.parquet")
    tw = tp.pivot_table(index="user_id", columns="topic", values="n", aggfunc="sum").fillna(0)
    ws["chat_msgs_classified"] = tw.sum(axis=1).reindex(ws.index).fillna(0)
    for t in tw.columns:
        ws[f"topic_{t}"] = tw[t].reindex(ws.index).fillna(0)
    return ws, tp


def thresholds(ws: pd.DataFrame) -> dict[str, float]:
    """Pre-registered: median among onboarded users who did the thing at all."""
    onb = ws[ws["onboarded"]]

    def med_pos(s: pd.Series) -> float:
        s = s[s > 0]
        return float(s.median()) if len(s) else np.inf

    return {
        "messages_per_week": med_pos(
            (
                onb[["outreach_texting_count", "outreach_robocall_count", "outreach_social_count"]]
                .fillna(0)
                .sum(axis=1)
            )
            / onb["weeks"]
        ),
        "field_actions": med_pos(
            onb[["outreach_door_knocking_count", "outreach_phone_banking_count"]].fillna(0).sum(axis=1)
        ),
        "lists_per_week": med_pos(onb["lists_built"].fillna(0) / onb["weeks"]),
        "opponent_days": med_pos(onb["days_opponents"]),
    }


def tech_comfort(ws: pd.DataFrame) -> pd.DataFrame:
    """Inferred tech comfort from three signals (see brief assumptions). Higher = more comfortable."""
    sig = pd.DataFrame(index=ws.index)
    howto = ws.get("topic_how to use the app", pd.Series(0, index=ws.index))
    has_chat = ws["chat_msgs_classified"] >= 3
    sig["howto_share"] = (howto / ws["chat_msgs_classified"]).where(has_chat)
    has_flows = ws["flow_starts"].fillna(0) >= 2
    sig["abandon_rate"] = (ws["flow_abandons"] / ws["flow_starts"]).where(has_flows)
    om = ws["onboarding_minutes"]
    sig["onboarding_minutes"] = om.where((om > 0) & (om <= 24 * 60))
    # Percentile within available users, flipped so that a high rank = comfortable.
    ranks = pd.DataFrame({c: 1 - sig[c].rank(pct=True) for c in sig.columns})
    n_avail = sig.notna().sum(axis=1)
    score = ranks.mean(axis=1).where(n_avail >= 2)
    lo, hi = score.quantile([1 / 3, 2 / 3])
    label = pd.Series("not observable", index=ws.index)
    label[score.notna()] = "middle"
    label[score >= hi] = "comfortable"
    label[score <= lo] = "less comfortable"
    sig["n_signals"] = n_avail
    sig["score"] = score
    sig["label"] = label
    return sig


def rule_labels(ws: pd.DataFrame, th: dict[str, float]) -> pd.DataFrame:
    lab = pd.DataFrame(index=ws.index)
    msgs = (
        ws[["outreach_texting_count", "outreach_robocall_count", "outreach_social_count"]]
        .fillna(0)
        .sum(axis=1)
    )
    lab["Heavy messenger"] = (msgs / ws["weeks"] >= th["messages_per_week"]) & (msgs > 0)
    field = ws[["outreach_door_knocking_count", "outreach_phone_banking_count"]].fillna(0).sum(axis=1)
    lab["Field organizer"] = (field >= th["field_actions"]) & (field > 0)
    lists = ws["lists_built"].fillna(0)
    lab["List builder"] = (lists / ws["weeks"] >= th["lists_per_week"]) & (lists > 0)
    lab["AI-led"] = (ws["share_ai"] >= 0.5) & (ws["days_ai"] >= 2)
    lab["Researcher"] = ((ws["days_opponents"] >= th["opponent_days"]) & (ws["days_opponents"] > 0)) | (
        ws["opp_research_started"].fillna(0) > 0
    )
    # Redefined 2026-10-28 after the checkpoint: the median rule collapsed to "any story action"
    # (median 1), so this mirrors AI-led instead.
    lab["Story and positions focused"] = (ws["share_story"] >= 0.5) & (ws["days_story"] >= 2)
    lab["Dabbler"] = (ws["areas_used"] == 1) & (ws["area_days"] <= 2)
    return lab


def choose_k(x: np.ndarray) -> tuple[int, pd.DataFrame]:
    rng = np.random.default_rng(SEED)
    rows = []
    for k in K_RANGE:
        base = KMeans(n_clusters=k, n_init=20, random_state=SEED).fit(x)
        sil = silhouette_score(x, base.labels_, sample_size=min(2000, len(x)), random_state=SEED)
        aris = []
        for b in range(20):
            idx = rng.choice(len(x), size=int(0.8 * len(x)), replace=False)
            m = KMeans(n_clusters=k, n_init=10, random_state=SEED + b).fit(x[idx])
            aris.append(adjusted_rand_score(base.labels_, m.predict(x)))
        sizes = np.bincount(base.labels_)
        rows.append(
            {
                "k": k,
                "silhouette": sil,
                "stability_ari": float(np.mean(aris)),
                "stability_min": float(np.min(aris)),
                "smallest": int(sizes.min()),
            }
        )
    tab = pd.DataFrame(rows)
    ok = tab[(tab["stability_ari"] >= 0.8) & (tab["smallest"] >= 30)]
    k = (
        int(ok.sort_values("silhouette", ascending=False)["k"].iloc[0])
        if len(ok)
        else int(tab.sort_values("stability_ari", ascending=False)["k"].iloc[0])
    )
    return k, tab


SHORT = {
    "outreach": "outreach",
    "voter_data": "voter data",
    "ai": "AI",
    "story": "story",
    "opponents": "opponent research",
}


def name_cluster(center: pd.Series) -> str:
    """Name a group by the areas it leans on (mean share >= 0.15) and the core areas it skips (< 0.05)."""
    share = center.rename(lambda c: c.removeprefix("share_")).sort_values(ascending=False)
    used = [a for a, v in share.items() if v >= 0.15]
    skipped = [SHORT[a] for a in ("outreach", "ai") if share[a] < 0.05]
    if share.iloc[0] >= 0.6 and len(used) == 1:
        label = f"{SHORT[used[0]]} only"
    elif len(used) >= 4:
        label = "everything but " + " or ".join(skipped) if skipped else "all-rounders"
    else:
        names = [SHORT[a] for a in used]
        label = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
        if skipped:
            label += ", no " + " or ".join(skipped)
    return label[0].upper() + label[1:]


def fit_groups(sub: pd.DataFrame) -> tuple[pd.Series, int, pd.DataFrame]:
    """Cluster on the square root of area shares; return named group per user, k, and the k table."""
    x = np.sqrt(sub[[f"share_{a}" for a in AREAS]].to_numpy())
    k, ktab = choose_k(x)
    km = KMeans(n_clusters=k, n_init=50, random_state=SEED).fit(x)
    cl = pd.Series(km.labels_, index=sub.index)
    centers = sub.groupby(cl)[[f"share_{a}" for a in AREAS]].mean()
    names = {c: name_cluster(centers.loc[c]) for c in centers.index}
    seen: dict[str, int] = {}
    for c in sorted(names, key=lambda c: -(cl == c).sum()):
        seen[names[c]] = seen.get(names[c], 0) + 1
        if seen[names[c]] > 1:
            names[c] = f"{names[c]} ({seen[names[c]]})"
    return cl.map(names), k, ktab


def eta_squared(values: pd.Series, groups: pd.Series) -> float:
    d = pd.DataFrame({"v": values, "g": groups}).dropna()
    grand = d["v"].mean()
    between = d.groupby("g")["v"].apply(lambda s: len(s) * (s.mean() - grand) ** 2).sum()
    total = ((d["v"] - grand) ** 2).sum()
    return float(between / total) if total else float("nan")


def cramers_v(a: pd.Series, b: pd.Series) -> float:
    t = pd.crosstab(a, b).to_numpy().astype(float)
    n = t.sum()
    exp = t.sum(1, keepdims=True) @ t.sum(0, keepdims=True) / n
    chi2 = ((t - exp) ** 2 / np.where(exp == 0, 1, exp)).sum()
    r, c = t.shape
    return float(np.sqrt(chi2 / (n * (min(r, c) - 1)))) if min(r, c) > 1 else float("nan")


def profile(df: pd.DataFrame, label: str) -> dict:
    n = len(df)
    senders = df[df["outreach_sent_count"].fillna(0) > 0]
    chat = df[df["chat_msgs_classified"] >= 1]
    topic_cols = [c for c in df.columns if c.startswith("topic_")]
    topics = (
        (chat[topic_cols].sum() / chat[topic_cols].sum().sum()).sort_values(ascending=False)
        if len(chat)
        else pd.Series()
    )
    p = {
        "group": label,
        "n": n,
        "onboarded": int(df["onboarded"].sum()),
        "activated": int(df["activated"].sum()),
        "pro_share": float(df["is_pro"].mean()),
        "experienced_share": float(df["experienced"].mean()),
        "office_city": float((df["office_level"] == "city").mean()),
        "median_days_on_win": float(df["days_on_win"].median()),
        "median_area_days_per_week": float(df["area_days_per_week"].median()),
        "share_never_used": {a: float((df[f"days_{a}"] == 0).mean()) for a in AREAS},
        "mean_mix": {a: float(df[f"share_{a}"].mean()) for a in AREAS},
        "sent_outreach_share": float(len(senders) / n) if n else float("nan"),
        "median_text_length": float(df["text_length_med"].median()),
        "median_text_audience": float(df["text_audience_med"].median()),
        "median_channels_if_sent": float(senders["outreach_channel_count"].median())
        if len(senders)
        else float("nan"),
        "median_first_send_days_before_election": float(senders["first_send_days_before_election"].median())
        if len(senders)
        else float("nan"),
        "median_lists_if_any": float(df.loc[df["lists_built"].fillna(0) > 0, "lists_built"].median()),
        "recommended_list_share": float(df["lists_recommended_share"].mean()),
        "median_filters_per_list": float(df["list_filters_avg"].median()),
        "chat_users_share": float(len(chat) / n) if n else float("nan"),
        "median_chat_msgs_if_any": float(chat["chat_msgs_classified"].median())
        if len(chat)
        else float("nan"),
        "top_topics": {k.removeprefix("topic_"): float(v) for k, v in topics.head(4).items()},
        "story_rewrite_users": float((df["story_rewrites"].fillna(0) > 0).mean()),
        "opp_research_started": float((df["opp_research_started"].fillna(0) > 0).mean()),
        "plan_task_users": float((df["plan_task_actions"].fillna(0) > 0).mean()),
    }
    return p


def journeys(df: pd.DataFrame) -> pd.Series:
    firsts = df[[f"first_{a}" for a in AREAS]]
    order = firsts.apply(
        lambda r: " > ".join(
            AREA_NAMES[c.removeprefix("first_")] for c in r.dropna().sort_values().index[:3]
        ),
        axis=1,
    )
    return order[order != ""].value_counts()


def main() -> None:
    ws, _ = load()
    run_ts = ws["run_ts"].iloc[0]
    say(
        f"Win usage archetypes, run {run_ts} UTC. Eligible users: {len(ws):,} "
        f"(onboarded {ws['onboarded'].sum():,}, activated {ws['activated'].sum():,}, Pro {ws['is_pro'].sum():,})."
    )

    # 1. Activity floor
    say("\n== 1. How much there is to describe ==")
    bands = pd.cut(ws["area_days"], [-1, 0, 2, 9, 1e9], labels=["0 area-days", "1-2", "3-9", "10+"])
    for cohort, sub in [("all", ws), ("onboarded", ws[ws["onboarded"]]), ("activated", ws[ws["activated"]])]:
        vc = bands[sub.index].value_counts(normalize=True).reindex(bands.cat.categories)
        say(f"  {cohort:<10} n={len(sub):>5}  " + "  ".join(f"{k}: {pct(v)}" for k, v in vc.items()))
    say(
        "  (area-days = distinct days with an action in outreach, voter data, AI assistant, story, opponents:"
        " sending, building, asking, rewriting, researching; page visits, onboarding, plan, paywall and"
        " compliance screens excluded)"
    )
    say("  share who opened each area vs share who did something there:")
    for a in AREAS:
        say(
            f"  {AREA_NAMES[a]:<18} opened {pct((ws[f'view_{a}'] > 0).mean())}  acted {pct((ws[f'days_{a}'] > 0).mean())}"
            f"  | onboarded opened {pct((ws.loc[ws['onboarded'], f'view_{a}'] > 0).mean())}"
            f" acted {pct((ws.loc[ws['onboarded'], f'days_{a}'] > 0).mean())}"
        )
    for a in AREAS:
        say(
            f"  acted in {AREA_NAMES[a]:<18} all {pct((ws[f'days_{a}'] > 0).mean())}"
            f"  onboarded {pct((ws.loc[ws['onboarded'], f'days_{a}'] > 0).mean())}"
            f"  activated {pct((ws.loc[ws['activated'], f'days_{a}'] > 0).mean())}"
            f"  | free {pct((ws.loc[~ws['is_pro'], f'days_{a}'] > 0).mean())} Pro {pct((ws.loc[ws['is_pro'], f'days_{a}'] > 0).mean())}"
        )

    # 2. Rule labels
    th = thresholds(ws)
    labels = rule_labels(ws, th)
    tc = tech_comfort(ws)
    say("\n== 2. Rule-based labels (overlapping; thresholds = median among onboarded users who did it) ==")
    say("  thresholds: " + ", ".join(f"{k}={v:.2f}" for k, v in th.items()))
    for c in labels.columns:
        m = labels[c]
        say(
            f"  {c:<26} all {m.sum():>4} ({pct(m.mean())})  onboarded {labels.loc[ws['onboarded'], c].sum():>4}"
            f"  activated {labels.loc[ws['activated'], c].sum():>4}  Pro share {pct(ws.loc[m, 'is_pro'].mean())}"
        )
    say(f"  {'Experienced':<26} all {ws['experienced'].sum():>4} ({pct(ws['experienced'].mean())})")
    nlab = labels.sum(axis=1)
    say(
        f"  labels per user: 0 = {pct((nlab == 0).mean())}, 1 = {pct((nlab == 1).mean())}, 2+ = {pct((nlab >= 2).mean())}"
    )
    co = labels.T.astype(int) @ labels.astype(int)
    say("  co-occurrence (users holding both):")
    say("    " + co.to_string().replace("\n", "\n    "))

    say("\n== 3. Tech comfort (inferred) ==")
    say(
        f"  signal coverage: chat how-to share {tc['howto_share'].notna().sum()}, abandon rate {tc['abandon_rate'].notna().sum()},"
        f" onboarding minutes {tc['onboarding_minutes'].notna().sum()}; scored (2+ signals) {tc['score'].notna().sum()}"
    )
    say("  signal correlations (Spearman, pairwise):")
    say(
        "    "
        + tc[["howto_share", "abandon_rate", "onboarding_minutes"]]
        .corr(method="spearman")
        .round(2)
        .to_string()
        .replace("\n", "\n    ")
    )
    say("  label counts: " + str(tc["label"].value_counts().to_dict()))
    for lab_name in ["comfortable", "less comfortable"]:
        m = tc["label"] == lab_name
        say(
            f"  {lab_name:<17} activated {pct(ws.loc[m, 'activated'].mean())}, AI user {pct((ws.loc[m, 'days_ai'] > 0).mean())},"
            f" experienced {pct(ws.loc[m, 'experienced'].mean())}, mobile share {ws.loc[m, 'mobile_share'].mean():.2f},"
            f" median area-days/week {ws.loc[m, 'area_days_per_week'].median():.2f}"
        )

    # 4. Groups, clustered separately for free and Pro (pooled clusters mostly restate Pro; see 5b)
    say(f"\n== 4. Usage groups, free and Pro clustered separately (users with {MIN_AREA_DAYS}+ area-days) ==")
    cl_pop = ws[ws["area_days"] >= MIN_AREA_DAYS].copy()
    cl_pop["group"] = ""
    tier_k = {}
    for tier, m in [("Pro", cl_pop["is_pro"]), ("Free", ~cl_pop["is_pro"])]:
        sub = cl_pop[m]
        g, tk, ttab = fit_groups(sub)
        cl_pop.loc[sub.index, "group"] = tier + ": " + g
        tier_k[tier] = {"k": tk, "n": len(sub), "k_table": ttab.to_dict("records")}
        say(
            f"  {tier}: {len(sub)} users; k selection:\n    "
            + ttab.round(3).to_string(index=False).replace("\n", "\n    ")
        )
        say(f"  {tier}: chose k={tk} (stability {ttab.set_index('k').loc[tk, 'stability_ari']:.2f})")
    say("  mean mix by group (groups under 30 users: counts only in write-ups):")
    mix = (
        cl_pop.groupby("group")[[f"share_{a}" for a in AREAS]]
        .mean()
        .rename(columns=lambda c: AREA_NAMES[c.removeprefix("share_")])
    )
    mix["n"] = cl_pop["group"].value_counts()
    say("    " + mix.round(2).sort_values("n", ascending=False).to_string().replace("\n", "\n    "))
    RESULTS["tier_k"] = tier_k
    RESULTS["group_mix"] = mix.round(3).to_dict("index")

    # 5. Falsification checks, within Pro (the free groups are too small to test)
    say("\n== 5. Is it more than volume or timing? (Pro groups) ==")
    pro = cl_pop[cl_pop["is_pro"]]
    say(
        f"  volume (log area-days/week) explained by group: eta^2 = {eta_squared(np.log1p(pro['area_days_per_week']), pro['group']):.2f}"
    )
    for ctx, sr in [
        ("tenure (days on Win)", pro["days_on_win"]),
        ("days to election at run", pro["days_to_election_at_run"].clip(-30, 400)),
    ]:
        say(f"  {ctx} explained by group: eta^2 = {eta_squared(sr, pro['group']):.2f}")
    for ctx in ["signup_month", "office_level", "experienced"]:
        say(f"  {ctx} vs group: Cramer's V = {cramers_v(pro[ctx].astype(str), pro['group']):.2f}")
    lab_c = labels.loc[cl_pop.index]
    lift = pd.DataFrame(
        {g: lab_c[cl_pop["group"] == g].mean() / lab_c.mean().replace(0, np.nan) for g in mix.index}
    ).T
    say(
        "  label lift by group (share holding label / share among all clustered users; >1.5 = over-represented):"
    )
    say("    " + lift.round(1).to_string().replace("\n", "\n    "))

    say("\n== 5b. Robustness ==")
    pooled, k, ktab = fit_groups(cl_pop)
    say(
        f"  pooled (free and Pro together): k={k}, stability {ktab.set_index('k').loc[k, 'stability_ari']:.2f}; "
        f"Pro vs pooled group Cramer's V = {cramers_v(cl_pop['is_pro'].astype(str), pooled):.2f} (why the tiers are split)"
    )
    say(
        "    "
        + pd.crosstab(pooled, cl_pop["is_pro"].map({True: "Pro", False: "free"}))
        .to_string()
        .replace("\n", "\n    ")
    )
    say(
        f"  agreement between pooled and per-tier groups: ARI {adjusted_rand_score(pooled, cl_pop['group']):.2f}"
    )
    vpop = ws[ws[[f"view_{a}" for a in AREAS]].sum(axis=1) >= MIN_AREA_DAYS]
    vtot = vpop[[f"view_{a}" for a in AREAS]].sum(axis=1)
    vx = np.sqrt(vpop[[f"view_{a}" for a in AREAS]].div(vtot, axis=0).to_numpy())
    vkm = KMeans(n_clusters=k, n_init=50, random_state=SEED).fit(vx)
    both = cl_pop.index.intersection(vpop.index)
    ari = adjusted_rand_score(pooled.loc[both], pd.Series(vkm.labels_, index=vpop.index).loc[both])
    say(
        f"  clustering on visit-days instead of action-days (pooled): {len(vpop):,} users clear the floor;"
        f" agreement with the action-day clusters on the {len(both):,} in both: ARI {ari:.2f}"
    )

    # 6. Group profiles, including the users below the clustering floor
    ws["group"] = "No area use"
    ws.loc[(ws["area_days"] > 0) & (ws["area_days"] < MIN_AREA_DAYS), "group"] = "Light touch (1-2 days)"
    ws.loc[cl_pop.index, "group"] = cl_pop["group"]
    say("\n== 6. Group profiles (all eligible users) ==")
    profs = []
    for g, sub in ws.groupby("group"):
        p = profile(sub, g)
        profs.append(p)
    profs.sort(key=lambda p: -p["n"])
    for p in profs:
        say(
            f"\n  [{p['group']}] n={p['n']} ({pct(p['n'] / len(ws))} of eligible); onboarded {p['onboarded']}, activated {p['activated']}"
            f"; Pro {pct(p['pro_share'])}; experienced {pct(p['experienced_share'])}; city office {pct(p['office_city'])}"
        )
        say(
            f"    median days on Win {p['median_days_on_win']:.0f}; median area-days/week {p['median_area_days_per_week']:.2f}"
        )
        say(
            "    never used: "
            + ", ".join(f"{AREA_NAMES[a]} {pct(v)}" for a, v in p["share_never_used"].items())
        )
        say(
            f"    outreach: sent {pct(p['sent_outreach_share'])}; channels if sent {p['median_channels_if_sent']}; "
            f"text length {p['median_text_length']:.0f} chars; text audience {p['median_text_audience']:.0f}; "
            f"first send {p['median_first_send_days_before_election']} days before election"
        )
        say(
            f"    voter data: lists if any {p['median_lists_if_any']}; recommended share {pct(p['recommended_list_share'])}; "
            f"filters per list {p['median_filters_per_list']}"
        )
        say(
            f"    AI: chat users {pct(p['chat_users_share'])}; msgs if any {p['median_chat_msgs_if_any']}; top topics "
            + ", ".join(f"{t} {pct(v)}" for t, v in p["top_topics"].items())
        )
        say(
            f"    story rewrites {pct(p['story_rewrite_users'])}; opponent research started {pct(p['opp_research_started'])}; "
            f"plan task actions {pct(p['plan_task_users'])}"
        )
    RESULTS["profiles"] = profs

    say("\n== 7. Groups among onboarded vs activated, split free vs Pro ==")
    order = [p["group"] for p in profs]
    tab = (
        pd.DataFrame(
            {
                "all": ws["group"].value_counts(normalize=True),
                "onboarded": ws.loc[ws["onboarded"], "group"].value_counts(normalize=True),
                "activated": ws.loc[ws["activated"], "group"].value_counts(normalize=True),
                "free": ws.loc[~ws["is_pro"], "group"].value_counts(normalize=True),
                "Pro": ws.loc[ws["is_pro"], "group"].value_counts(normalize=True),
            }
        )
        .reindex(order)
        .fillna(0)
    )
    say("    " + (tab * 100).round(0).astype(int).astype(str).add("%").to_string().replace("\n", "\n    "))
    say(
        "    n: all {}, onboarded {}, activated {}, free {}, Pro {}".format(
            len(ws), ws["onboarded"].sum(), ws["activated"].sum(), (~ws["is_pro"]).sum(), ws["is_pro"].sum()
        )
    )

    say("\n== 8. Journeys: order areas were first used (top 8 paths, first three areas) ==")
    for cohort, sub in [("onboarded", ws[ws["onboarded"]]), ("activated", ws[ws["activated"]])]:
        j = journeys(sub)
        say(f"  {cohort} (users with any area use: {j.sum()})")
        for path, n in j.head(8).items():
            say(f"    {n:>4}  {path}")
    snd = ws[ws["first_outreach"].notna()]
    to_out = (snd["first_outreach"] - pd.to_datetime(snd["signed_up_at"]).dt.normalize()).dt.days
    say(
        f"  days from signup to first outreach action: median {to_out.median():.0f}, p25 {to_out.quantile(.25):.0f}, p75 {to_out.quantile(.75):.0f} (n={len(to_out)})"
    )
    firsts = ws[[f"first_{a}" for a in AREAS]].fillna(pd.Timestamp("2100-01-01"))
    first_area = firsts.idxmin(axis=1).where(ws["area_days"] > 0)
    fa = first_area.str.removeprefix("first_").map(AREA_NAMES)
    act_by_first = (
        ws.assign(first_area=fa)
        .dropna(subset=["first_area"])
        .groupby("first_area")
        .agg(n=("activated", "size"), activated=("activated", "mean"), pro=("is_pro", "mean"))
    )
    say("  activation by first area used (descriptive, not causal):")
    say("    " + act_by_first.sort_values("n", ascending=False).round(2).to_string().replace("\n", "\n    "))

    say("\n== 9. Chat topics overall (classified messages, totals only) ==")
    tcols = [c for c in ws.columns if c.startswith("topic_")]
    tot = ws[tcols].sum().sort_values(ascending=False)
    for c, v in tot.items():
        say(
            f"  {c.removeprefix('topic_'):<30} {int(v):>5} ({pct(v / tot.sum())}); users {int((ws[c] > 0).sum())}"
        )
    for cohort, m in [("experienced", ws["experienced"]), ("newcomer", ~ws["experienced"])]:
        s = ws.loc[m, tcols].sum()
        say(
            f"  {cohort}: "
            + ", ".join(
                f"{k.removeprefix('topic_')} {pct(v / s.sum())}"
                for k, v in s.sort_values(ascending=False).head(5).items()
            )
        )

    out = ws.reset_index()[["user_id", "group", "onboarded", "activated", "is_pro"]].join(
        labels.reset_index(drop=True)
    )
    out["tech_comfort"] = tc["label"].to_numpy()
    out.to_parquet(DATA / "assignments.parquet", index=False)
    RESULTS.update(
        {
            "run_ts": run_ts,
            "pooled_k": k,
            "pooled_k_table": ktab.to_dict("records"),
            "thresholds": th,
            "group_shares": tab.to_dict(),
        }
    )
    (HERE / "results.txt").write_text("\n".join(OUT_LINES) + "\n")
    (HERE / "results.json").write_text(json.dumps(RESULTS, indent=2, default=str))


if __name__ == "__main__":
    sys.exit(main())
