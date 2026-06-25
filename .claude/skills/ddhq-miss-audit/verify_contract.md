# Verify a batch of DDHQ misses

You verify whether gp_api product users actually ran for office in the 2026 cycle, and what
happened, for candidacy records that did NOT match DDHQ election results. For EACH record in
your batch, do targeted web research and produce one result object.

## Input fields per record
`unique_id, fn (first name), ln (last name), state, dt (product election date), office,
office_level, category (our deterministic pre-class), won, ns_n, ns_won, any_office`

- `category` is our SQL pre-classification, NOT ground truth. Confirm or correct it.
- `any_office` (when present) = an office in DDHQ for this state held by SOMEONE with this
  exact name. It may be a namesake, or it may be the same person under a different office.
- The product `dt` is often a hard-coded general-election date and can be WRONG (wrong stage
  or mis-entered). The real election may be on a nearby date. Do not trust it for filtering.

## What to search (stop when confident)
1. Ballotpedia: `<first> <last> <office> <state> ballotpedia`
2. County/city/town clerk or election-office results page for that locality
3. Local news / candidate website / social: `"<first> <last>" <office or city> <state> 2026 election results`

## BROADEST possible "absent" check (REQUIRED before calling anyone a non-candidate or absent)
Before concluding a person did not run / is absent from results, search broadly for the NAME
ANYWHERE in the state, not just the office we have on file:
- `"<first> <last>" <state> 2026 election` and `"<first> <last>" <state> candidate`
- Our office string may be wrong. The person may have actually run for a DIFFERENT office or
  in a DIFFERENT town in the same state. If you find them running/winning ANY race in the
  state, that is NOT a non-candidate — capture the real office in `actual_office`.
- If `any_office` is populated, explicitly check whether that DDHQ office is the SAME PERSON
  (same locality/region, plausible) or a NAMESAKE (different part of the state, common name).

## Decide, per record
- ran: did this person actually run for office in the 2026 cycle anywhere in the state?
- made_ballot: did they appear on an actual ballot (vs only exploring / never filed)?
- result: won | lost | withdrew | not_on_ballot | unknown
- office_level: federal | state | county | municipal | school | special_district | local | unknown.
  Descriptive only, for auditing. Do NOT use it to guess whether DDHQ covers the race —
  whether DDHQ has the race is determined separately and empirically (we contract DDHQ to
  cover small races, so size does not predict coverage).
- name_found_elsewhere_in_state: yes | no — did the broad state-wide name search find this
  same person under a different office/town than the one we have?

Do NOT decide whether DDHQ "should" cover the race. That is resolved upstream from DDHQ
staging, not from your judgment of office size.

## final_category (pick exactly one — candidacy status / outcome only, coverage-agnostic)
- `WON`            ran and WON. Requires a source naming THIS person as winner of THIS race.
- `LOST`           ran and on the ballot, confirmed lost.
- `ON_BALLOT`      confirmed on the ballot, outcome unknown / not yet decided.
- `NOT_ON_BALLOT`  filed / declared / campaigned but withdrew or missed filing; never on a ballot.
- `NON_CANDIDATE`  no evidence anywhere that this person ran. Likely a product signup who never filed.

## Output
Write a JSON array (one object per input record) to the results path given in your prompt,
AND return that same JSON array as your final message. Object shape:
```json
{
  "unique_id": "gp_api|123",
  "ran": "yes|no|unknown",
  "made_ballot": "yes|no|unknown",
  "result": "won|lost|withdrew|not_on_ballot|unknown",
  "office_level": "federal|state|county|municipal|school|special_district|local|unknown",
  "name_found_elsewhere_in_state": "yes|no",
  "actual_office": "real office if different from ours, else null",
  "actual_election_date": "real date if known and different from ours, else null",
  "final_category": "WON|LOST|ON_BALLOT|NOT_ON_BALLOT|NON_CANDIDATE",
  "source_url": "https://...",
  "confidence": "high|med|low",
  "notes": "one line of evidence"
}
```

## Rules
- Never guess a win. `result=won` requires a source naming THIS person as the winner of THIS
  (or a confirmed equivalent) race.
- Common names collide. Confirm locality/office before trusting a hit. A different person with
  the same name winning elsewhere is NOT a win for this record.
- If after all tiers (including the broad state-wide search) you find nothing, return
  ran=unknown, final_category=NON_CANDIDATE, confidence=low. Do not invent a result.
- Return ONLY the JSON array as your final message. No prose outside it.
