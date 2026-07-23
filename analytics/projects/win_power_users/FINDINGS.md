# Win power-user profile: findings (DATA-2153)

Prepared 2026-07-22. Overall figures are the frozen 2026-07-22 values. The three-period cuts
were refreshed from live data on 2026-07-23; the engaged total across periods (2,602) differs
from the frozen headline (2,598) by four candidacies, an immaterial live-data drift.
Source notebook: `notebooks/01_power_user_profile.ipynb`.

Scope: Win candidacies with a general election in 2025 or 2026, constituency of at least
1,000 voters, latest non-demo campaign version. One row per candidacy. 23,244 candidacies.

## What we set out to answer

Bryan's question was whether the people who use the Win product most are a distinctive,
recognizable group we could profile and then go acquire more of. That power users exist is
not in doubt. The real question was whether they cluster into segments sharp enough to define
a lookalike acquisition target for DATA-2154.

That breaks into two questions this report answers in order:

- What does the average power user look like? The absolute composition of the cohort, so we
  know who these people are for messaging and understanding.
- Are they distinct in any way we can use to distinguish them? The over-index against a
  baseline, so we know which traits actually set power users apart and can be handed to
  acquisition.

### How we defined a power user

Power use is a usage signal, kept deliberately separate from Pro (a commercial signal). We
used two nested cohorts:

- Engaged: sent at least one voter outreach campaign, or viewed the dashboard heavily (top
  decile). 2,598 candidacies, about 11 percent of the base.
- Intense: sent at least five outreach campaigns. 304 candidacies, about 1 percent. This is a
  subset of engaged, the deep-usage core.

We report every cut two ways: against the full base (who are our power users across the whole
candidate list) and against the active-only population of 7,892 candidacies that did anything
at all (among people who did something, what makes some become power users). The second
comparator matters because 66 percent of the base is dormant, meaning it never touched the
product. Comparing only against the full base would partly just measure who signed up versus
who did not.

### The headline

**Power users do cluster, but as a moderate tilt, not a bright line.** The distinctive signals are
**in the range of 1.2 to 2 times the baseline rate, not an order of magnitude.** There is a real
profile to hand to acquisition, but it should be described as a lean, not a rule.

## What the average power user looks like

This section answers the first question. It is the absolute composition of the cohort, not an
over-index. A segment can be common without being distinctive, so the portrait and the
distinctiveness view (next section) do not have to agree, and they do not.

### Overall portrait

The intense power user (the deep-usage core, 304 candidacies) is **most often a city-council
challenger in a mid-size district.**

- By office: city council 49 percent, mayor 14 percent, school board 11 percent, county
  supervisor 8 percent.
- Where incumbency is known: challenger 54 percent, incumbent 12 percent.
- Runs at the city level, median constituency around 19,000 voters, mostly 5,000 to 100,000.
- Spread across the country. Georgia, Texas, Ohio, California, and Washington lead, no single
  state above about 10 percent.
- 71 percent inside today's ICP, 31 percent upgraded to Pro, wins about 40 percent of decided
  races.

The broader engaged cohort (2,598 candidacies) looks similar but flatter: city council 36
percent, school board 15 percent, mayor 11 percent, congressional 8 percent; a median
constituency around 27,000 voters; 61 percent ICP; 20 percent Pro.

| attribute | engaged | intense |
|---|---|---|
| candidacies (N) | 2,598 | 304 |
| median constituency (voters) | 27,197 | 18,884 |
| inside today's ICP | 61% | 71% |
| upgraded to Pro | 20% | 31% |
| win rate (decided generals) | 39% | 40% |

Provenance: cohorts engaged (N=2,598) and intense (N=304); absolute composition, no
comparator; frozen 2026-07-22 values.

The office table makes the common-versus-distinctive tension concrete. It puts each office's
share of the power-user cohort next to its over-index. City council fills 36 percent of engaged
users but over-indexes at only 1.07, so it is common, not distinctive. Mayor is 11 percent of
engaged users yet over-indexes at 1.3. **This is the direct answer to "are power users mayors
just because we have a lot of mayors": no.** The over-index divides out how many of each office
we have, so an office that is merely common lands near 1.0.

| office | base N | base share | % of engaged | % of intense | over-index vs base | over-index vs active |
|---|---|---|---|---|---|---|
| City Council | 7,779 | 33.5% | 35.9% | 49.0% | 1.07 | 1.07 |
| School Board | 4,321 | 18.6% | 14.9% | 11.2% | 0.80 | 0.98 |
| Congressional | 1,912 | 8.2% | 8.2% | 3.9% | 1.00 | 0.86 |
| Mayor | 1,889 | 8.1% | 10.5% | 13.8% | 1.30 | 1.23 |
| Other | 1,662 | 7.2% | 4.8% | 3.3% | 0.68 | 0.80 |
| County Supervisor | 1,431 | 6.2% | 7.8% | 7.6% | 1.27 | 1.04 |
| State House | 1,243 | 5.3% | 6.5% | 4.3% | 1.22 | 1.07 |
| Statewide/Governor | 641 | 2.8% | 3.3% | 1.6% | 1.21 | 0.88 |

Provenance: cohort engaged (N=2,598) for shares, intense (N=304) for the intense column;
over-index vs full base and vs active-only; base N shown per row; N>=30 floor met on every
row. Frozen 2026-07-22 values.

So the portrait is mostly a reflection of what is common on the platform, while the over-index
signals in the next section are what actually set power users apart. For messaging and
understanding who these people are, use the portrait. For acquisition, lean on the distinctive
signals.

### Portrait by period

We split the window into three periods: pre-November 2025 (odd-year local generals, January to
October 2025), November 2025 (the completed cycle), and 2026 (mostly upcoming). Adoption rose
over time. Read the pre-November 2025 portrait as anecdotal, since it has only 85 engaged and
14 intense candidacies and is dominated by a single state.

| period | base N | engaged N | engaged rate | intense N | intense rate |
|---|---|---|---|---|---|
| pre-Nov 2025 | 3,993 | 85 | 2.1% | 14 | 0.4% |
| Nov 2025 | 9,352 | 1,117 | 11.9% | 179 | 1.9% |
| 2026 | 9,899 | 1,400 | 14.1% | 111 | 1.1% |

Provenance: cohorts engaged and intense within each period; rate is cohort N over period base
N; full-base counts; no floor rule (all periods above 30). Refreshed 2026-07-23.

The engaged cohort by period. Composition percentages are shares within the period cohort.

| period | N | top offices | challenger share | median voters | ICP | Pro | win rate (decided) |
|---|---|---|---|---|---|---|---|
| pre-Nov 2025 | 85 | City Council 58%, Mayor 26%, School Board 9% | 69% | 7,173 | 86% | 29% | 37% (n=71) |
| Nov 2025 | 1,117 | City Council 55%, School Board 21%, Mayor 14% | 57% | 13,366 | 70% | 16% | 39% (n=965) |
| 2026 | 1,400 | City Council 20%, Congressional 15%, County Supervisor 13%, State House 12% | 12% | 66,690 | 51% | 22% | 35% (n=142) |

Provenance: cohort engaged split by period; absolute composition within each period cohort, no
comparator; N shown per row; pre-Nov 2025 (n=85) below reliable-read size, treat as anecdotal.
Refreshed 2026-07-23. In 2026 incumbency is unclassified for 85 percent of engaged candidacies,
so the low challenger share reflects missing labels on upcoming races, not incumbents.

The intense cohort by period. Small N throughout, so read as directional.

| period | N | top offices | challenger share | median voters | ICP | Pro | win rate (decided) |
|---|---|---|---|---|---|---|---|
| pre-Nov 2025 | 14 | City Council 71%, Mayor 29% | 79% | 11,212 | 100% | 36% | 36% (n=14) |
| Nov 2025 | 179 | City Council 58%, Mayor 16%, School Board 15%, County Supervisor 5% | 74% | 11,837 | 78% | 28% | 41% (n=169) |
| 2026 | 111 | City Council 32%, County Supervisor 14%, State House 11%, Congressional 11% | 18% | 50,735 | 57% | 34% | 30% (n=23) |

Provenance: cohort intense split by period; absolute composition within each period cohort, no
comparator; N shown per row; pre-Nov 2025 (n=14) and the 2026 win-rate (n=23) are very small,
treat as directional. Refreshed 2026-07-23.

**The type of power user shifted upmarket over time, but mostly because of the election
calendar, not changed behavior.** In November 2025 the typical power user was a city-council,
school-board, or mayoral challenger in a small-to-mid district, median around 13,000 voters. In
2026 the mix moves to congressional, county supervisor, and state house candidacies, a median
district around 51,000 to 67,000 voters, and a lower ICP share. Most of that is odd-year local
races in 2025 versus a midterm year in 2026, plus the fact that upcoming 2026 races are not yet
classified, so incumbency and office level read as unknown for most of them. It is not clean
evidence that behavior changed. The pre-November 2025 period is 100 percent Alabama in the
intense cohort and 72 percent Alabama in the engaged cohort, so its portrait is anecdotal and
should not be compared.

## What distinguishes power users

This section answers the second question. The over-index divides each segment against its own
base rate, so a segment that is merely common does not read as a signal. Over 1 means
over-represented among power users.

### Overall

1. **Challengers, not incumbents. This is the single clearest and most durable signal.**
   Challengers over-index at 1.28 against the full base, and **the signal gets stronger, not
   weaker, when we control for sign-up: 1.57 against the active population, and 2.07 in the
   intense core.** Incumbents under-index throughout. This is on mission. The people leaning on
   the product hardest are outsiders trying to unseat someone, not sitting officeholders.

| incumbency | base N | engaged vs base | engaged vs active | intense vs base |
|---|---|---|---|---|
| challenger | 6,047 | 1.28 | 1.57 | 2.07 |
| incumbent | 4,066 | 0.47 | 0.88 | 0.66 |
| unknown | 13,131 | 1.03 | 0.84 | 0.61 |

Provenance: cohorts engaged and intense; over-index vs full base and vs active-only; base N
shown per row; N>=30 floor met on every row. Frozen 2026-07-22 values.

2. **Geography holds up.** A consistent set of states over-indexes in both comparators, including
   Maine, New Hampshire, Colorado, and Georgia. Unlike office and constituency size, the state
   signal survives the active-only comparison, so it is a genuine targeting axis rather than a
   sign-up artifact. Full state tables are in the descriptive notes.

3. **Office and constituency size are mostly a sign-up effect.** Against the full base, mayor,
   county supervisor, and state house over-index, and school board and clerk roles under-index.
   But these over-indexes compress to roughly parity once we compare within the active
   population, as the office table above shows in its last column. Those offices predict who
   joins the product, not who becomes a power user. Treat them as weak signals, not headlines.

#### What is not signal

The three largest raw over-indexes in the data are not acquisition targets and should not be
read as the profile.

- Pro status has the highest raw lift, but Pro is a conversion that happens after signup, so
  you cannot acquire a lookalike on it. It is also partly baked into the engaged definition
  (Pro users view the dashboard a lot). We report Pro as context and deliberately keep it out
  of the segment search.
- Missing-data buckets (unknown election level, unscored viability) over-index because recent
  or unclassified candidacies are both more active and more likely to have blanks. These are
  coverage artifacts, not segments.

### By period

The distinguishing signals per period, with counts and Wilson 95 percent intervals so the
uncertainty on small cells is visible. Comparator is the full base within each period.

**Challenger is the distinguishing signal in every period, and it strengthens as the cohort
deepens and as the calendar moves toward 2026.** Engaged challenger over-index runs 1.38, 1.48,
2.87 across the three periods; intense runs 1.56, 1.91, 4.34.

Engaged, challenger over-index by period:

| period | challenger base N | in cohort | cell rate | over-index | 95% CI |
|---|---|---|---|---|---|
| pre-Nov 2025 | 2,007 | 59 | 2.9% | 1.38 | 2.3 to 3.8% |
| Nov 2025 | 3,629 | 641 | 17.7% | 1.48 | 16.5 to 18.9% |
| 2026 | 411 | 167 | 40.6% | 2.87 | 36.0 to 45.4% |

Provenance: cohort engaged; over-index vs full base within period; base N shown per row; all
cells above the N>=30 floor. Refreshed 2026-07-23. The 2026 challenger base is only 411 of 9,899
candidacies, since most 2026 races are not yet classified, so the high over-index rests on that
classified slice.

Intense, challenger over-index by period:

| period | challenger base N | in cohort | cell rate | over-index | 95% CI |
|---|---|---|---|---|---|
| pre-Nov 2025 | 2,007 | 11 | 0.5% | 1.56 | 0.3 to 1.0% |
| Nov 2025 | 3,629 | 133 | 3.7% | 1.91 | 3.1 to 4.3% |
| 2026 | 411 | 20 | 4.9% | 4.34 | 3.2 to 7.4% |

Provenance: cohort intense; over-index vs full base within period; base N shown per row; cells
above the N>=30 floor on the challenger base. Refreshed 2026-07-23. The intense cohort is small
(14, 179, 111 by period), so read the pre-Nov 2025 row as directional.

Office over-index by period (engaged), rows above the N>=30 floor:

| period | office | base N | in cohort | cell rate | over-index | 95% CI |
|---|---|---|---|---|---|---|
| pre-Nov 2025 | Mayor | 446 | 22 | 4.9% | 2.32 | 3.3 to 7.4% |
| pre-Nov 2025 | City Council | 1,573 | 49 | 3.1% | 1.46 | 2.4 to 4.1% |
| pre-Nov 2025 | School Board | 1,316 | 8 | 0.6% | 0.29 | 0.3 to 1.2% |
| Nov 2025 | Mayor | 1,015 | 159 | 15.7% | 1.31 | 13.6 to 18.0% |
| Nov 2025 | City Council | 4,807 | 609 | 12.7% | 1.06 | 11.8 to 13.6% |
| Nov 2025 | School Board | 2,165 | 239 | 11.0% | 0.92 | 9.8 to 12.4% |
| 2026 | Mayor | 428 | 93 | 21.7% | 1.54 | 18.1 to 25.9% |
| 2026 | City Council | 1,399 | 277 | 19.8% | 1.40 | 17.8 to 22.0% |
| 2026 | Congressional | 1,902 | 214 | 11.3% | 0.80 | 9.9 to 12.8% |

Provenance: cohort engaged; over-index vs full base within period; base N shown per row; rows
with base N<30 in the period omitted. Refreshed 2026-07-23.

Mayor is the most consistent office signal, over-indexing in every period. City council tracks
near or just above parity, in line with the overall read that it is common rather than
distinctive. School board under-indexes. The office picture per period does not overturn the
overall conclusion that office is a weaker axis than incumbency.

## What power users are not (the anti-profile)

The inverse of the profile is as clear as the profile. Power users are not incumbents
(incumbents under-index at 0.47). They are especially not city-level incumbents (0.27) or
candidates in the smallest 1,000 to 5,000 voter city races (0.45). **If the profile is a
mid-district challenger, the anti-profile is a small-town incumbent.**

## The counterintuitive planning finding

**Power users win less.** Among decided completed generals, the base wins 57 percent of the time,
but engaged users win 38 percent and the intense core 40 percent. Some of this is a
composition effect, because the base is full of dormant incumbents who win easily and never
need the tool. But **the gap survives when we compare within incumbency status:** engaged
incumbents win 69 percent versus 82 percent for incumbents overall, and engaged challengers win
31 percent versus 45 percent for challengers overall. So the effect is real, not purely an
artifact. **The honest read is that power users are strivers in harder races, not the people
already positioned to win.** Absolute win levels should be treated as directional, since they
depend on which races get a recorded result.

| cohort | all decided | incumbents | challengers |
|---|---|---|---|
| base | 57.2% (n=11,003) | 82.0% (n=3,833) | 44.6% (n=5,289) |
| engaged | 38.5% (n=1,178) | 68.9% (n=196) | 31.4% (n=729) |
| intense | 39.8% (n=206) | 63.6% (n=33) | 35.9% (n=153) |

Provenance: cohorts base, engaged, intense; decided completed generals only; N shown per cell;
within-incumbency columns stratify the base-vs-cohort comparison. Frozen 2026-07-22 values.

## Implications for ICP (DATA-2154)

**Power users skew toward today's ICP but do not sit entirely inside it.** The ICP share rises from
55 percent of the base to 60 percent of engaged users to 71 percent of the intense core. That
leaves **about 29 percent of the intense core outside today's ICP definition,** mostly school
board and congressional candidacies. This is a soft signal for an ICP revisit rather than a
clear instruction, because school board also under-indexes on engagement overall, so its
presence here is partly a function of its large raw count.

## Other descriptive notes

- **Usage intensity: half the engaged cohort actually sent zero outreach** and qualified on
  dashboard use alone (median campaigns sent is 0, 90th percentile is 5). The intense cohort is
  the real outreach core. If we want a cohort that is unambiguously "used the core product to
  reach voters," intense is it.
- **Pro does not lift winning.** Crossing Pro with power-user status, the 60 percent headline
  win-rate belongs entirely to the dormant "neither" quadrant. Every engaged quadrant wins about
  38 to 40 percent whether or not the candidate went Pro. Pro and winning are not linked here.
- Geography is moderately concentrated. The top 10 states hold about half of power users, led by
  Ohio, California, Georgia, and Texas. **Georgia is the standout: both high-volume (6.5 percent
  of all power users) and strongly over-indexing (1.8 times the base rate), so it is the clearest
  place to look for more.** The two views are worth reading together, because volume and
  distinctiveness are not the same thing.

Top states by volume (share of engaged power users):

| state | engaged N | % of engaged |
|---|---|---|
| OH | 202 | 7.8% |
| CA | 181 | 7.0% |
| GA | 170 | 6.5% |
| TX | 166 | 6.4% |
| NC | 127 | 4.9% |
| MA | 113 | 4.3% |
| WA | 103 | 4.0% |
| MI | 101 | 3.9% |

Provenance: cohort engaged (N=2,598); share of the engaged cohort, not an over-index; frozen
2026-07-22 values.

Top states by over-index (base N at least 30):

| state | base N | over-index | cell rate 95% CI |
|---|---|---|---|
| ME | 150 | 1.91 | 15.5 to 28.6% |
| GA | 838 | 1.81 | 17.7 to 23.1% |
| CO | 439 | 1.71 | 15.7 to 23.1% |
| LA | 66 | 1.63 | 10.7 to 29.1% |
| AR | 240 | 1.57 | 13.2 to 22.8% |
| NE | 82 | 1.53 | 10.5 to 26.6% |
| WY | 32 | 1.40 | 6.9 to 31.8% |
| TN | 435 | 1.36 | 12.1 to 18.8% |

Provenance: cohort engaged; over-index vs full base; base N shown per row; N>=30 floor applied.
Frozen 2026-07-22 values.

Georgia is the one state that appears near the top of both tables, which is why it is the
standout. Maine and Colorado over-index hard but carry far less volume.

## Methodology and caveats

- Distribution versus distinctiveness. The over-index divides each segment against its own base
  rate, so a segment that is merely common (city council, school board) does not read as a
  signal. That handles the "is this just our user mix" question one dimension at a time. It does
  not isolate each factor net of the others: if mayors also tend to be challengers, the
  single-dimension view cannot say how much of "mayor" is really "challenger." The combination
  sweep in the notebook looks at joint cells (office by state by incumbency) as a partial check,
  but it is still descriptive. A multivariate model that holds the other dimensions constant is
  deferred to DATA-2154.
- Overall figures are the frozen 2026-07-22 run. The three-period cuts were recomputed on the
  2026-07-23 refresh with the same helper and cohort definitions; the engaged total across
  periods (2,602) differs from the frozen headline (2,598) by four candidacies, which does not
  move any read.
- Usage counts are lifetime, not anchored to the election date, so for completed 2025 races a
  little post-election activity can count toward the power-user definition. We confirmed the Pro
  flag has only a 1 percent leak of this kind; a full check on the usage counts is deferred.
- Tenure could not be built as intended. Prior-run history exists for only about 1.6 percent of
  candidates in the warehouse, so we substituted incumbency status. The full first-timer versus
  re-election versus long-term split needs a web-search backfill, which is deferred.
- Party was dropped. GoodParty only onboards non-major-party candidates, so party is nearly
  constant by design and carried no usable signal.
- This profile is descriptive and associational, not causal.

## What is next

- DATA-2154 turns the challenger and geography signals into an acquisition target and revisits
  the ICP definition using the out-of-ICP office list.
- DATA-2171 reruns these cuts on the full Win population as an unbiased control on the
  sales-selection bias in the Pro segment.
