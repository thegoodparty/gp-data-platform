# Analysis principles

What a finding has to be before anyone sees it. These apply at every stage of the loop: framing should aim for them, the results checkpoint and the reviewers should test against them. The mechanics that carry them out (cohorts, binning, Wilson intervals) are in `methodology.md`; the everyday practices and gotchas (staff exclusion, dating numbers, rounding, row-count checks) are in [practices.md](practices.md).

This is the shared copy of the analytics owner's personal `data-analysis-principles` skill, which also covers work outside this repo. Change both together.

## 1. Fewer, actionable facts
- A few facts someone can act on beat a wall of interesting ones nobody can.
- The whole result fits in a sentence or two. If it doesn't, the analysis isn't finished.
- Answer the question that was asked. Measures nobody asked for are where wrong conclusions come from, and they skip the checks the original question got.
- Each fact names who would act on it. If nobody would, cut it.

## 2. No fact exists in isolation
- A number means nothing without a comparison. 50 users tells you nothing; 50 users against 10 last year tells you growth.
- Default comparison is the same period last year. For a group, compare with everyone else; for one person, with people like them.
- Use the best comparison available, and say which way it leans. Records (payments, accounts, sends) compare as is. Changed features compare at the level of the job they do (AI assistant and AI chat are both "got AI help"). Other behavior compares as active days, over events tracked in both years. When none of these fit, show this year only and say why. Name the direction of any known bias; a comparison is usable when the finding holds despite it.
- State the comparison next to the number, not in a footnote.
- Comparing years: cut anything cumulative at the same point in both, and split by sales-created vs self-signup. 2025 ran on volume-driven sales and a different product, so compare rates over a denominator below the top of the funnel (onboarded, not all signups).

## 3. Check what a group is made of before crediting the difference
- A gap between two groups is often just a gap in who is in them.
- Before saying X drives Y, split by the obvious variables (signup channel, activation, incumbency, election date) and see if the gap survives.
- Pick the denominator the decision applies to, minus people who never had the chance. Stages after onboarding divide by the onboarded; a rate over all signups moves with who signed up, not with the product. A view narrowed by the outcome itself (activated users when the question is outreach) comes second, never as the headline: a price paid by every customer is sized on every customer.
- Before crediting a change over time, list what else changed in the same window (new flows, launches, sales motions) and name any that could share the credit.
- Break a composite headline metric into its parts before quoting it. Most of the 2026 "outreach" gain over 2025 was the 10DLC texting-registration form, not more outreach.
- Example: what moved Pro users on Win only made sense once channel and activation were in view. The challenger depth gap disappeared once we accounted for composition.
- Don't filter on something the outcome causes and then study the outcome's other causes. That produces confident, backwards findings.

## 4. If it takes a correction step to see, it doesn't ship
- Ship what reads off a single cross-tab with large groups.
- Large effects on large groups hold through every check. Small effects that only appear after an adjustment keep flipping.
- Corrections go in a methods note as "what we checked and ruled out". That shows diligence; it is not a finding.

## 5. Say how sure you are, and say "we don't know" when we don't
- "We can't answer this yet" is an acceptable answer, and better than a confident half-baked conclusion. Don't be afraid to say it.
- When the answer is "we don't know", say why, and what it would take to find out (the data, the time, the experiment).
- Mark each finding as measured or inferred.
- Run the significance test before ranking or ordering anything, not after someone asks.
- If the recommendation keeps changing with each new cut, stop and simplify. Don't ship the latest reading as a verdict.
- Before numbers go to other people, have them rerun independently (a person, or an agent with no context).

## 6. One definition per concept
- Use the governed or canonical definition (activated, onboarded, active user). Never invent a proxy for one analysis.
- If the canonical one doesn't work for some period or group, write down a standard workaround and reuse it everywhere.
- If you have to deviate, say so and say why, next to the number.
- Cheap test: split a measure by the governed flag it is named after. The opposite group must read zero.

## Keeping these current
- At the end of every analysis, ask one question: did this work teach us something that should add, sharpen, merge or remove a principle here, or a practice in `practices.md`? "No change" is the usual answer and a fine one.
- Removing counts as much as adding. Before proposing a new principle, say which existing one it merges with or replaces. Stay at about six; a seventh needs a reason none of the six cover.
- Also look for principles nothing has tested in a while, or that the last few analyses broke without harm. Those are candidates to cut.
- In an analytics-process run this is part of the ledger read-back in `calibration.md`. Propose the change and wait for approval; change this copy and the personal skill together.
