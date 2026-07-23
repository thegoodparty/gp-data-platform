# Draft reply to Victoria's questions on the topline table (relayed by Amanda, 2026-07-23)

Paste-ready for the Slack thread. Numbers are from the 2026-07-23 re-run (small
restatements vs the 07-22 table are expected; flags are as-of-run-date).

---

Dug into all of this. Answers below, in order.

**1. Are the activated and Pro rows mutually exclusive?**

**No, they're overlapping flags on the same accounts, and neither is a subset of the other.** Nov 2025 cohort: 441 users were both Pro and activated at their election, 312 Pro only, 248 activated only.

**2. "Historically the only way to activate was to become Pro"**

**Victoria is essentially right, and it's still true today.** We checked the product code and its git history: real voter contact (text, robocall, door knocking, phone banking) has been Pro-gated since June 2025, voter file since Aug 2024, and gating only got tighter over time. Social posts are the only free channel. The 248 "activated but not Pro" users are measurement, not a free tier: mostly ex-Pros who churned after their election, plus Pros we can't link to billing (comped upgrades, or Stripe under a different email).

**3. Is our activation rate 3% for upcoming elections?**

**Yes: 309 of 9,383 registered accounts with an upcoming 2026 election have activated so far.** Two caveats: those candidates still have time (Nov 2025 finished at 5.2%), and the denominator is every account created. Among onboarded users, activation runs ~17-21%.

**4. Does the 60.1% W+1 retention apply only to that small activated subset?**

**Yes, by construction: the KR starts the clock at a user's first outreach send, so it only measures activated ICP candidates.** The two numbers chain rather than conflict: registered -> activated (3-5%) -> ~60% of those return in week 1. Weekly cohorts are small, roughly 10-25 ICP activations per week.

**5. Bryan's Pro -> Activated ratio**

**Agreed, and it's now a standing table in the report: 58.6% of Nov 2025 at-election Pros had activated by election day** (441 of 753; summer 2025: 30.2%; 2026 held: 54.5%). The gap, Pros who pay but never send, is the actionable number.

**6. Pro upgrade date (the double-click)**

**Confirmed: the product DB stores no reliable Pro-upgrade timestamp, and even with two new linkage channels, ~54% of current Pros can't be dated by any source.** What exists is a "last Pro flip" JSON field (~46% coverage, overwritten on cancellations) and two 2026-only fields; Stripe remains the dating source. The undatable Pros cluster in Jan-Jul 2025 accounts, consistent with comped/admin grants (no billing record) plus billing-email mismatches (~590 live Stripe subscriptions match no account). Recommendation: ask eng for an immutable first-Pro timestamp or isPro audit table.
