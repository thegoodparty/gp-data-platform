# Civics Mart

GoodParty's canonical political/civics data — candidates, candidacies, elections, and results.

All models materialize as tables in the `mart_civics` schema.

## Models

| Model | Grain | Description |
|---|---|---|
| `users` | user_id | GP application users with aggregate campaign and organization counts |
| `campaigns` | campaign_version_id | GP campaigns with historical version tracking |
| `organizations` | organization_slug | Links users to Win campaigns or Serve elected offices |
| `candidacy` | gp_candidacy_id | Candidacies from HubSpot archive (2025) and BallotReady (2026+) |
| `candidate` | gp_candidate_id | Unique persons, deduped across BallotReady and HubSpot |
| `candidacy_stage` | gp_candidacy_stage_id | Per-stage election results (primary, general, runoff) |
| `election` | gp_election_id | Full election cycles with pivoted stage dates |
| `election_stage` | gp_election_stage_id | Individual election stages with vendor IDs |

## Key Concepts

- **Candidate** = a person. **Candidacy** = a person running for a specific office in a specific year.
- A candidate can have many candidacies. A candidacy belongs to exactly one election.
- **Candidacy Stage** = results for one candidacy in one election phase (primary, general, etc.)
- **Election** vs **Election Stage**: an election encompasses all stages; election_stage is one phase.

## Which Table Do I Need?

```
What are you looking for?
+-- A person's info (name, email, state)        --> candidate
+-- What office they ran for, election dates     --> candidacy
+-- Did they win or lose? Vote counts?           --> candidacy_stage
+-- Election details (office, district, dates)   --> election / election_stage
+-- GP app user data (signup, Win/Serve status)  --> users
+-- GP campaign data (verified, pledged, pro)    --> campaigns
+-- Is the office in our target market (ICP)?    --> candidacy.is_win_icp
+-- Win/Serve product linkage                    --> organizations
```

## Joining Models

### Candidacy with candidate details and election results

```sql
select
    cy.gp_candidacy_id,
    c.full_name,
    cy.candidate_office,
    cy.candidacy_result,
    cs.election_result,
    cs.votes_received,
    es.stage_type,
    es.election_date
from mart_civics.candidacy cy
inner join mart_civics.candidate c
    on cy.gp_candidate_id = c.gp_candidate_id
left join mart_civics.candidacy_stage cs
    on cy.gp_candidacy_id = cs.gp_candidacy_id
left join mart_civics.election_stage es
    on cs.gp_election_stage_id = es.gp_election_stage_id
```

### Link candidacy to GP product campaign

```sql
select
    cy.gp_candidacy_id,
    cy.product_campaign_id,
    cam.campaign_office,
    cam.user_email,
    cam.is_verified
from mart_civics.candidacy cy
inner join mart_civics.campaigns cam
    on cy.product_campaign_id = cam.campaign_id
    and cam.is_latest_version
```
