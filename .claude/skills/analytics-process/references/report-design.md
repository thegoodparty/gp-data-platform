# Report design

Part of the **analytics-process** skill. How a report or findings page reads to the people it is
for. [findings-page.md](findings-page.md) sets the layout and the visual rules; this file sets how
the content is written and cut so a reader outside the data team can use it.

Learned on the 2026 Win engagement report: each review round added a correct cut or caveat to the
page, and together they made it unreadable for its audience.

## Who reads it

Assume sales, go-to-market, product and the CPO. They know the product, not the data. If someone
who built the analysis struggles to read a chart, these readers will skip it.

## One question, one answer, one chart

- Each section is one question, answered in one plain sentence, shown in one chart.
- Splits, alternative cuts, caveats and method notes go in collapsed appendix sections, linked from
  the section.
- A chart that needs a paragraph to explain it is a chart to rework, not a subtitle to lengthen.

## The plain-reader test

Before every publish, ask of each chart and table:

1. What question does it answer?
2. Could a sales lead say its answer in one sentence without hovering over anything?
3. Does it need more than two lines of subtitle?

If 1 or 2 fails, or 3 is yes, rework it or move it to the appendix. Then run the plain-reader
review ([pipeline.md](pipeline.md#plain-reader-review)).

## Where review findings go

Reviewers check rigor; the page has to be understood. Each finding from any of the three reviews
(data scientist, product manager, plain reader) gets one of two homes:

- **The page**, only if it changes the answer a reader takes away.
- **The appendix or methods**, otherwise. This is the default.

Fix the number everywhere either way; the question is only where the explanation lives.

## Words

- Define a term at the top of the section that uses it, and wherever it first appears on the page.
- Plain words over field or team names: "reached voters", not "activated"; "our candidate success
  team (CaS)" on first use.
- One count per concept across the page. When a chart uses a different base (a funnel that starts
  at a tracking date, a channel only available part of the year), say so in its subtitle with both
  numbers.
- Keep formats readers already know from earlier reports (power users are read as ratios against
  the average). Ask before changing one.

## Charts and tables

- Every chart with more than one series has a visible legend. Values sit next to the marks;
  hovering only adds detail.
- One comparison color scheme across the page: blue for the group in focus, grey for the
  comparison. Other colors only for a third or fourth series.
- Tables of percentages get cell shading by value. When a column carries the comparison (a ratio,
  "times as likely"), shade that column.
- Show a recent period as final when no more data is coming, even if a fixed window has not fully
  elapsed (for example, sends after the elections have passed).

## Check the headline numbers

Before building on a number, check it against what its label implies. A "top 15%" group that is
half the population, or two counts of the same thing that disagree, means the definition or the
base is wrong. Raise it before writing the sentence around it.
