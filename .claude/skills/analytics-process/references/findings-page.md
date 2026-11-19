# Findings page

An optional deliverable: a single HTML page that reports an executed analysis to people who will
not open the notebook (product leads, PMs, colleagues). It is published as a private Claude
artifact and shared from there. Choose it at framing (`execution_notes.preferred_format`), always
in addition to a notebook or script, which the results checkpoint and the reviewers read.

Write for a CPO, not an analyst. The main body is the answer and a few findings, each with one
chart; everything an analyst needs to check the work goes in an appendix below it. Fewer, better
facts beat a wall of tables: if a reader would need to ask "how do I read one row", it belongs in
the appendix.

Worked examples: `analytics/projects/win_pro_votes_needed/build_page.py` (DATA-2809) and
`build_segments_page.py` in the same folder (DATA-2810).

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
   (population, count, window, run timestamp, exclusions, and the team the page serves), and a
   status line: draft or reviewed, which reviews are pending, owner.
2. **The answer.** One or two sentences in bold, with no supporting bullets. Then links to each
   finding and to the analyst appendix.
3. **Findings**, two to four, one per thing the reader must take away. Each has a heading phrased
   as a question, a one-sentence answer with live numbers, one chart, and a bold
   "For <decision>:" line saying what it means for the decision the brief names (for example
   "For pricing:"). No tables, SQL or caveats beyond one line here.
4. **What we don't know.** The open questions that limit the answer (cause vs correlation,
   missing data), one short paragraph each, plainly stated.
5. **Recommendations.** "What should product do with this?" Three or four numbered items, each
   opening with a bold action, then the evidence in one sentence, then a link to its finding.
   Until the reviews land, say they are the analyst's read. The last item says where to look next
   (often a follow-up ticket). Merge items that make the same point.
6. **Appendix for analysts**, under a page-level heading so the break is obvious. It holds
   everything else, each part as a question-headed section: every cut and its table, explorers,
   cross-tabs, the SQL and Python in collapsed `<details>` blocks, robustness (each check from the
   brief and whether the answer held), rerun (the commands, and a note that numbers move between
   runs, so quote the run timestamp), the question as asked (the requester's words, quoted, with
   name and date), the framing from the brief (decision, question, hypothesis, population,
   definitions, window, comparison, falsification, known concerns), the **Assumptions** table, and
   the full brief YAML in a collapsed block.

The Assumptions table lists every decision that shaped the numbers, in the order it was made,
including amendments and corrections after a readout: the decision, why, its effect on the
results, and who decided it.

## Charts in the main body

- A title above each chart saying what is measured ("Median texts per texter, by votes needed to
  win"), and a one-line guide to reading it.
- Positive framing: show who did the thing ("did any outreach"), not who did not.
- Group labels say what is in the group ("City-level seats: council, school board, mayor", not
  "city").
- Never show a group whose comparison group is mostly unknowns or missing data.
- Compute any gap quoted in the text from the values as the chart shows them, so the prose and
  the chart never differ by a rounding point.

## Design

The page follows the GoodParty dashboard rules. The ones that matter for a findings page:

- Ink goes to data. No cards, boxes or shadows; whitespace and a hairline rule separate sections.
- GoodParty look: Outfit font, navy `#0B1529`, blue `#0048C2`, light and dark mode, sentence case.
- **Text fills the column.** No `max-width` in `ch` on paragraphs, notes, lists or the answer, and no
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
