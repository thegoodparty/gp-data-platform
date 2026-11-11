# Good practices and gotchas

The mechanics behind the principles in [analysis-principles.md](analysis-principles.md). These aren't philosophy: each one comes from a time it cost us.

This is the shared copy of the `practices.md` in the analytics owner's personal `data-analysis-principles` skill. Change both together.

## Populations
- **Always exclude staff** (`lower(email) like '%@goodparty.org'`) from every user count. Some governed metrics include them on purpose, so the semantic layer won't do it for you.
- **State the grain.** Per user, per person, per candidacy and per account give different counts. Say which one.

## Statistics
- **Plain medians and shares, not averages.** On a skewed count, means can show a gap that medians don't (690 vs 1,613 on means, 263 vs 221 on medians).
- **Distributions are shares.** Counts go alongside, not in place of them.
- **Small groups get flagged, not ranked.** A rate on fewer than about 100 people is a hint, not a finding.
- **Rates count only people old enough for the window.** A cohort still filling up is labeled as partial, never shown as a low rate.
- **Active means 2+ days when comparing years.** One day with any event makes whichever year records more events look better (the Nov 2026 all-hands deck: a 1-day bar flattered 2026).

## Numbers that travel
- **Date every number:** which environment and when it was measured. Never put dev numbers under a production heading.
- **Round counts in anything that has to last** ("about 8,400"). Keep exact figures only where they record a decision.
- **Pin the timestamp** beside numbers from tables that refresh hourly; they move within a session.

## Checking the work
- **A dropping row count is a regression** until proven otherwise, however green the tests are. Compare with the previous count.
- **Measure a proposed fix against the data**, not just the bug it fixes. Run the old rule and the new one over the real column and diff them.
- **A search that finds nothing isn't proof something is absent.** Confirm with a second, differently shaped method.
