# Findings page

An optional deliverable: a single HTML page that reports an executed analysis to people who will
not open the notebook (product leads, PMs, colleagues). It is published as a private Claude
artifact and shared from there. Choose it at framing (`execution_notes.preferred_format`), alone
or with a notebook or script.

Worked example: `analytics/projects/win_pro_votes_needed/build_page.py` (DATA-2809).

## When it is built

After the reviewers report, not before: the page carries recommendations, and those should be
reviewed. A draft page may be published earlier when the requester asks for one, with its status
line saying the reviews are pending.

Build it with a `build_page.py` in the project folder that imports the same `analyze.py`
functions as the notebook, so every number on the page comes from the code that produced the
notebook, and the page rebuilds after a rerun. Never type a number into the page by hand; write
it from a variable.

## Layout, top to bottom

1. **Header.** An eyebrow (ticket, purpose, "internal"), the question as the title, one meta line
   (population, count, window, run timestamp, exclusions), and a status line: draft or reviewed,
   which reviews are pending, owner.
2. **TL;DR.** The one-sentence answer in bold, then four to seven supporting points, each ending
   with a link to its section. Then a row of links to every section.
3. **Recommendations.** "What should product do with this?" Three or four numbered items, each
   opening with a bold action, then the evidence in one or two sentences, then a link to its
   section. Until the reviews land, say they are the analyst's read. The last item says where to
   look next (often a follow-up ticket). Merge items that make the same point rather than listing
   every implication.
4. **Sections**, one per part of the question, each with: an eyebrow number, a heading phrased as
   a question, a one-paragraph takeaway that answers it with live numbers, the figures, and the
   SQL and Python behind them in collapsed `<details>` blocks.
5. **Robustness.** The checks from the brief (composition, sensitivity, excluded groups), each
   stating whether the answer held.
6. **Rerun.** The commands that reproduce the page, and a note that numbers move between runs, so
   quote the run timestamp.
7. **Appendix.** The question as asked (the requester's words, quoted, with name and date), the
   framing from the brief (decision, question, hypothesis, population, definitions, window,
   comparison, falsification, known concerns), the **Assumptions** table, and the full brief YAML
   in a collapsed block.

The Assumptions table lists every decision that shaped the numbers, in the order it was made,
including amendments and corrections after a readout: the decision, why, its effect on the
results, and who decided it.

## Design

The page follows the GoodParty dashboard rules. The ones that matter for a findings page:

- Ink goes to data. No cards, boxes or shadows; whitespace and a hairline rule separate sections.
- GoodParty look: Outfit font, navy `#0B1529`, blue `#0048C2`, light and dark mode, sentence case.
- **Text fills the column.** No `max-width` in `ch` on paragraphs, notes, lists or the TL;DR, and no
  `text-wrap: balance` on paragraphs. The page column's own width is the only limit. Text cut to
  two thirds of the column is the most repeated review comment on these pages.
- Compare groups with a dot plot against a dashed "everyone" line, not a wide table of rates. The
  rate sits on the mark, with "x of y" in the tooltip. Fade marks resting on fewer than 100
  people; under 30, show the count only.
- A comparison across cohorts (all vs a subgroup, before vs after) gets one subsection per cohort
  with an identical layout, stacked vertically, so the eye reads straight down. Never a table that
  hides columns behind a horizontal scroll.
- One-line takeaways in a smaller size (0.88rem) across the full width. Plain words, never column
  names. Separate several values in one label with a muted "|".
- Bars share one scale across panels. No dual axes.

Before publishing, grep the stylesheet for `ch;` and check the page at phone width.
