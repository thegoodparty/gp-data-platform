"""Assemble and execute notebooks/win_pro_votes_needed.ipynb for DATA-2809.

Run from analytics/ after build_working_set.py:
    uv run --with pyarrow --with nbformat --with nbclient --with ipykernel \
        python projects/win_pro_votes_needed/build_notebook.py
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
    md("""# Win Pro usage by votes needed to win (DATA-2809)

Input to Win pricing and packaging: does the size of a race change which features Pro users use, and how
much outreach they send and buy?

**Who:** paying Win Pro customers whose election is in 2026: Pro today, or canceled on or after their
election day. Staff, demo accounts and comped (never-paid) Pro excluded. **Window:** the six months before the
election, election day included. **Spec:** `win_pro_votes_needed_brief.yaml`.
**Votes needed:** rebuilt with the rule gp-api serves live (half the projected turnout, plus one, from the
override district, the picked race, or the position). Every rate shows its count; groups under 30 show
counts only. Association only: nothing here says race size causes the behavior."""),
    code("""import sys
from pathlib import Path
import pandas as pd
pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)
sys.path.insert(0, str(Path.cwd()))
import analyze as A  # noqa: E402

ws = A.load()
print(f"Run {ws['run_ts'].iloc[0]} UTC. Pro users with a 2026 election: {len(ws):,}")
print(ws.pro_status.value_counts().to_string())"""),
    md("## 1. Votes needed: where it comes from and how much to trust it"),
    code("""display(A.coverage(ws))
print(A.sanity(ws))
display(A.band_sizes(ws))"""),
    md("""## 2. Feature use by race size

Races from July 15 only: before then the action events record nothing in outreach, story or opponent research
(older event names), though people in those races texted."""),
    code("fm = ws[ws.feature_measured]\ndisplay(A.feature_table(fm))\ndisplay(A.feature_depth(fm))"),
    md("""Larger races look less active, but most 10,000+ users signed up before July 2026, when the AI assistant and
opponent research were new or missing. Held within signup era, the band differences mostly disappear:"""),
    code("""for f in ["acted_opponents", "acted_ai", "acted_outreach"]:
    print(f)
    display(A.within(fm, f, "signup_era"))"""),
    md(
        "Pro status is a check, not a headline: most of those who canceled after their election had spring races, before the AI assistant and opponent research existed."
    ),
    code("""for f in ["acted_opponents", "texted", "over_bundle"]:
    print(f)
    display(A.within(ws, f, "pro_status"))"""),
    md("## 3. Outreach channels by race size"),
    code("display(A.channel_table(ws))"),
    md("## 4. Texting volume and spend by race size"),
    code("""display(A.volume_table(ws))
t = ws[ws.texts_sent > 0]
print("Median texts per texter, within signup era:")
display(t.groupby(["band", "signup_era"], observed=True)["texts_sent"].agg(["count", "median"]).unstack())
print("Over 5,000 texts, within signup era (lower bound, then upper bound counting unknown counts as over):")
display(A.within(ws, "over_bundle", "signup_era"))
display(A.within(ws, "over_bundle_upper", "signup_era"))
print("Texts that cover 50/75/90% of each band:")
display(A.allowance_table(ws))"""),
    code("""p = ws[ws.paid]
top = p.outreach_usd.max()
print(f"Largest single spender: ${top:,.0f} of ${p.outreach_usd.sum():,.0f} ({top / p.outreach_usd.sum():.0%}).")
print(f"Total without them: ${p.outreach_usd.sum() - top:,.0f}. Medians above are unaffected.")
print(f"Median dollars per text among payers who texted: ${(p.outreach_usd / p.texts_sent.where(p.texts_sent > 0)).median():.3f}")"""),
    md("## 5. Robustness"),
    code("""for name, mask in [("without races before Jul 15", ~ws.early_election), ("without races after the run date", ~ws.future_election)]:
    print(name)
    display(A.volume_table(ws[mask]))
print("10,000+ band by office level:")
b = ws[ws.band == "10,000+"]
display(b.groupby("office_level").agg(users=("user_id", "size"), median_votes_needed=("votes_needed", "median"),
        texted=("texted", "sum"), over_5k=("over_bundle", "sum")))"""),
]

nb = nbf.v4.new_notebook(
    cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3"}}
)
NotebookClient(nb, timeout=300, kernel_name="python3", resources={"metadata": {"path": str(HERE)}}).execute()
out = NB_DIR / "win_pro_votes_needed.ipynb"
nbf.write(nb, out)
print(f"wrote {out}")
