"""Assemble and headlessly execute notebooks/usage_archetypes.ipynb for DATA-2690.

Run from analytics/ after analyze.py:
    uv run --with scikit-learn --with pyarrow --with matplotlib --with nbformat --with nbclient --with ipykernel \
        python projects/win_usage_archetypes/build_notebook.py
"""

from pathlib import Path

import nbformat as nbf
from nbclient import NotebookClient

HERE = Path(__file__).resolve().parent
NB_DIR = HERE / "notebooks"
NB_DIR.mkdir(exist_ok=True)

md = nbf.v4.new_markdown_cell
code = nbf.v4.new_code_cell

cells = [
    md("""# How candidates use Win: usage groups (DATA-2690, v1)

Describes how Win candidates use the product beyond counts: how much they act, which areas they combine,
what they skip, and in what order they pick things up. Results only, no election outcomes (those are
DATA-2678, which uses these groups as one of its cuts).

**Who:** Win candidates who signed up from 2026-07-01 with at least 14 days on Win before the earlier of
their election or the run date. Staff, demo accounts and non-latest campaigns excluded.
**Spec:** `usage_archetypes_brief.yaml` (decisions from the 2026-10-28 checkpoint are at the bottom).
**Inputs:** `datasets/` written by `build_working_set.py`; groups and labels from `analyze.py`."""),
    code("""import json, sys
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

HERE = Path.cwd()
sys.path.insert(0, str(HERE))
import analyze as A  # noqa: E402

ws, _ = A.load()
asg = pd.read_parquet(HERE / "datasets" / "assignments.parquet").set_index("user_id")
ws = ws.join(asg[["group"]])
res = json.loads((HERE / "results.json").read_text())
pd.set_option("display.max_colwidth", 60)
print(f"Run {res['run_ts']} UTC. Eligible users {len(ws):,}: onboarded {ws['onboarded'].sum():,}, "
      f"activated {ws['activated'].sum():,}, Pro {ws['is_pro'].sum():,}.")"""),
    md("""## Definitions

- **Action-day:** a distinct day on which the user did something in an area: sent or drafted outreach,
  built or exported a voter list, asked the AI assistant, requested a story rewrite or edited an issue,
  or started opponent research. Page views, onboarding, the campaign plan, paywalls and compliance
  screens don't count.
- **Areas:** outreach, voter data, AI assistant, story and issues, opponent research.
- **Onboarded:** viewed the dashboard within 14 days of signup. **Activated:** the governed definition
  (`users_win_base.is_activated`, sent outreach)."""),
    md("## 1. Most candidates barely act"),
    code("""bands = pd.cut(ws["area_days"], [-1, 0, 2, 9, 1e9], labels=["None", "1-2 days", "3-9 days", "10+ days"])
cohorts = {"All": ws.index, "Onboarded": ws.index[ws["onboarded"]], "Activated": ws.index[ws["activated"]],
           "Free": ws.index[~ws["is_pro"]], "Pro": ws.index[ws["is_pro"]]}
tab = pd.DataFrame({k: bands[v].value_counts(normalize=True).reindex(bands.cat.categories) for k, v in cohorts.items()}).T
ax = (tab * 100).plot.barh(stacked=True, figsize=(8, 3), color=["#d9d9d9", "#9ecae1", "#4292c6", "#08519c"])
ax.set_xlabel("% of users")
ax.invert_yaxis()
ax.legend(title="Action-days", bbox_to_anchor=(1, 1))
ax.set_title("Action-days per user")
plt.tight_layout()
plt.show()
(tab * 100).round(0).assign(n=[len(v) for v in cohorts.values()])"""),
    md("## 2. Opening an area isn't using it (onboarded users)"),
    code("""onb = ws[ws["onboarded"]]
oa = pd.DataFrame({
    "Opened": [(onb[f"view_{a}"] > 0).mean() * 100 for a in A.AREAS],
    "Did something": [(onb[f"days_{a}"] > 0).mean() * 100 for a in A.AREAS],
}, index=[A.AREA_NAMES[a] for a in A.AREAS])
ax = oa.plot.barh(figsize=(7, 3), color=["#bdbdbd", "#2171b5"])
ax.invert_yaxis()
ax.set_xlabel(f"% of onboarded users (n={len(onb)})")
plt.tight_layout()
plt.show()
oa.round(0)"""),
    md("## 3. Free users act almost only in the AI assistant and story"),
    code("""fp = pd.DataFrame({
    "Free": [(ws.loc[~ws["is_pro"], f"days_{a}"] > 0).mean() * 100 for a in A.AREAS],
    "Pro": [(ws.loc[ws["is_pro"], f"days_{a}"] > 0).mean() * 100 for a in A.AREAS],
}, index=[A.AREA_NAMES[a] for a in A.AREAS])
ax = fp.plot.barh(figsize=(7, 3), color=["#fdae6b", "#2171b5"])
ax.invert_yaxis()
ax.set_xlabel(f"% who did something there (free n={(~ws['is_pro']).sum()}, Pro n={ws['is_pro'].sum()})")
plt.tight_layout()
plt.show()
fp.round(0)"""),
    md("""## 4. Usage groups, free and Pro grouped separately

Users with 3+ action-days, grouped by how their action-days split across the five areas (k-means on the
square root of each area's share; k picked for stability on resampling, then fit). Free and Pro are
grouped separately because a pooled grouping mostly sorts out who is Pro (section 6). Groups under 30
users are shown as counts only."""),
    code("""mix = pd.DataFrame(res["group_mix"]).T
mix = mix.sort_values("n", ascending=False)
shares = mix.drop(columns="n")
fig, ax = plt.subplots(figsize=(8, 4))
im = ax.imshow(shares.to_numpy(), cmap="Blues", vmin=0, vmax=0.9)
ax.set_xticks(range(shares.shape[1]), shares.columns, rotation=30, ha="right")
ax.set_yticks(range(len(shares)), [f"{g} (n={int(n)})" for g, n in mix["n"].items()])
for i in range(shares.shape[0]):
    for j in range(shares.shape[1]):
        v = shares.iat[i, j]
        ax.text(j, i, f"{v:.0%}", ha="center", va="center", color="white" if v > 0.45 else "black", fontsize=8)
ax.set_title("Share of each group's action-days by area")
plt.tight_layout()
plt.show()
print({t: f"k={v['k']}, n={v['n']}" for t, v in res["tier_k"].items()})"""),
    md("## 5. What each group does and skips"),
    code("""rows = []
for p in res["profiles"]:
    small = p["n"] < 30
    r = {"group": p["group"], "n": p["n"], "activated": p["activated"]}
    if not small:
        r.update({
            "sent outreach": f"{p['sent_outreach_share']:.0%}",
            "AI chat users": f"{p['chat_users_share']:.0%}",
            "story rewrite": f"{p['story_rewrite_users']:.0%}",
            "opp. research started": f"{p['opp_research_started']:.0%}",
            "never used": ", ".join(A.AREA_NAMES[a] for a, v in p["share_never_used"].items() if v >= 0.5) or "-",
            "action-days/week": round(p["median_area_days_per_week"], 2),
        })
    rows.append(r)
pd.DataFrame(rows).set_index("group").astype(object).fillna("count only")"""),
    code("""order = [p["group"] for p in res["profiles"]]
gs = pd.DataFrame(res["group_shares"]).reindex(order) * 100
gs.round(0).astype(int).astype(str) + "%\""""),
    md("""## 6. Are the groups more than payment, volume or timing?

- Pooled across free and Pro, the groups line up strongly with being Pro, which is why they are split.
- Within Pro, the groups differ in volume (expected: some do more) but hardly by tenure or how close the
  election is."""),
    code("""print("\\n".join(line for line in (HERE / "results.txt").read_text().splitlines()
                if line.startswith(("== 5", "  volume", "  tenure", "  days to election", "  signup_month",
                                 "  office_level", "  experienced vs", "  pooled", "  agreement", "  clustering on visit"))))"""),
    md("## 7. Journeys: which area candidates use first"),
    code("""for name, sub in [("onboarded", ws[ws["onboarded"]]), ("activated", ws[ws["activated"]])]:
    print(name)
    print(A.journeys(sub).head(8).to_string())
    print()
firsts = ws[[f"first_{a}" for a in A.AREAS]].fillna(pd.Timestamp("2100-01-01"))
fa = firsts.idxmin(axis=1).where(ws["area_days"] > 0).str.removeprefix("first_").map(A.AREA_NAMES)
(ws.assign(first_area=fa).dropna(subset=["first_area"]).groupby("first_area")
   .agg(n=("activated", "size"), activated=("activated", "mean"), pro=("is_pro", "mean"))
   .sort_values("n", ascending=False).round(2))"""),
    md("""Activation by first area is descriptive only: outreach-first users are mostly Pro, so this is not a
claim that starting with outreach causes activation."""),
    md("## 8. What candidates ask the AI assistant (totals only)"),
    code("""tcols = [c for c in ws.columns if c.startswith("topic_")]
tot = ws[tcols].sum().sort_values()
ax = (tot / tot.sum() * 100).rename(lambda c: c.removeprefix("topic_")).plot.barh(figsize=(7, 4), color="#2171b5")
ax.set_xlabel(f"% of {int(tot.sum()):,} classified messages")
plt.tight_layout()
plt.show()"""),
    md("""Topics are a majority vote of three models over each message, run in the warehouse; only per-user
counts leave it. 23% land in "other", not yet characterized."""),
    md("""## 9. What we checked and left out

- **Rule-based labels.** Heavy messenger, field organizer, list builder and researcher are 91 to 95% Pro,
  so they restate Pro. "Story and positions focused" (redefined at the checkpoint) holds 12 users.
- **Tech comfort.** Three inferred signals (how-to share of AI questions, abandoned flows, onboarding
  speed) don't agree with each other (pairwise correlations 0.13 or less), so no label is reported. An
  open problem, with no good leads yet.
- **Visit-days instead of action-days** give different groups (agreement 0.14), so the choice to count
  actions matters. Actions are used because most outreach and voter-data visits involve no action."""),
    code("""print("\\n".join(line for line in (HERE / "results.txt").read_text().splitlines()
                if line.startswith(("  Heavy", "  Field", "  List", "  AI-led", "  Researcher", "  Story and", "  Dabbler"))))
tc = A.tech_comfort(ws)
tc[["howto_share", "abandon_rate", "onboarding_minutes"]].corr(method="spearman").round(2)"""),
]

nb = nbf.v4.new_notebook(
    cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3"}}
)
out = NB_DIR / "usage_archetypes.ipynb"
NotebookClient(nb, timeout=600, kernel_name="python3", resources={"metadata": {"path": str(HERE)}}).execute()
nbf.write(nb, out)
print(f"wrote {out}")
