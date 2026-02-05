# MBAN 2026 Mart

## Models

| Model | Grain | Description |
|---|---|---|
| `candidates_outreach` | hubspot_id x outreach_id | HubSpot candidacies with text outreach campaigns |
| `mban_election_results` | race_id x candidate_id | DDHQ election results (pass-through) |
| `mban_candidacy_election_results` | hubspot_id x election_date x election_type | AI-matched DDHQ election links per candidacy |
| `deid_voters` | LALVOTERID | De-identified nationwide voter data |

## Joining `mban_candidacy_election_results`

This model is a bridge table that links HubSpot candidacies to DDHQ elections via AI matching. It intentionally excludes columns available in the other MBAN models to avoid redundancy. Use the join keys below to enrich it.

### Get candidacy details + election match

Join to `candidates_outreach` on `hubspot_id` for candidate name, office, state, etc.

```sql
select
    co.*,
    cer.election_type,
    cer.has_match,
    cer.match_confidence,
    cer.match_reasoning,
    cer.ddhq_race_id,
    cer.ddhq_candidate_id
from candidates_outreach as co
left join mban_candidacy_election_results as cer
    on co.hubspot_id = cer.hubspot_id
```

### Get election match + DDHQ results (votes, winner status)

Join to `mban_election_results` on `ddhq_race_id` + `ddhq_candidate_id` for vote totals and winner status.

```sql
select
    cer.*,
    er.candidate,
    er.race_name,
    er.votes,
    er.is_winner,
    er.is_uncontested,
    er.total_number_of_ballots_in_race
from mban_candidacy_election_results as cer
left join mban_election_results as er
    on cer.ddhq_race_id = er.race_id
    and cer.ddhq_candidate_id = er.candidate_id
```

### Full join: candidacy + match + results

```sql
select
    co.hubspot_id,
    co.state,
    co.candidate_office,
    co.election_date,
    cer.election_type,
    cer.has_match,
    cer.match_confidence,
    er.candidate as ddhq_candidate,
    er.race_name as ddhq_race_name,
    er.votes,
    er.is_winner,
    er.total_number_of_ballots_in_race
from candidates_outreach as co
left join mban_candidacy_election_results as cer
    on co.hubspot_id = cer.hubspot_id
left join mban_election_results as er
    on cer.ddhq_race_id = er.race_id
    and cer.ddhq_candidate_id = er.candidate_id
```
