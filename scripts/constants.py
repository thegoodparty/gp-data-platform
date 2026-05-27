# scripts/constants.py
"""Shared constants for entity resolution configs."""

OFFICE_STOP_WORDS = (
    "'city','of','the','county','board','council','school','district',"
    "'mayor','alderperson','trustee','at','large','zone','ward','seat',"
    "'position','commission','precinct','town','village','member',"
    "'councilmember','supervisor','supervisors','commissioner','judge',"
    "'branch','education','unified','public','elementary','consolidated',"
    "'central','special','independent','office','clerk','treasurer',"
    "'coroner','sheriff','magistrate','property','value','administrator',"
    "'emergency','services','director','justice','peace','representative',"
    "'house','representatives','legislature','legislative','metro',"
    "'and','for','no.','odd','unexpired'"
)

# Shared post-prediction filter: requires name + identity signal + office overlap.
# Each config can extend this with entity-specific clauses.
#
# Office overlap is now satisfied by gamma_official_office_name > 0, which the
# CustomComparison's ArrayIntersectLevel (over official_office_name_tokens)
# fires on cross-source naming variants like DDHQ "Lincoln County R-IV School
# District" ↔ BR "Winfield R-4 School Board". Token normalization (parens
# preserved, roman→arabic, "no. N" → "r-N") lives in the dbt office_name_tokens
# macro so the inline string-split fallback that used to live here is no
# longer needed.
BASE_POST_PREDICTION_FILTER = """
    gamma_last_name > 0
      AND (gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0)
      AND gamma_official_office_name > 0
"""

# EO-specific post-prediction filter: adds contact-info bypass and office_type
# fallback for cross-source office title synonyms. Contact-confirmed pairs
# (email or phone match) skip office checks entirely since identity is established.
# Does NOT include the candidacy-specific br_race_id guard.
EO_POST_PREDICTION_FILTER = f"""
    gamma_last_name > 0
      AND (gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0)
      AND (
        gamma_email > 0
        OR gamma_phone > 0
        OR gamma_official_office_name > 0
        OR list_has_any(
          list_filter(
            string_split(lower(official_office_name_l), ' '),
            x -> len(x) > 1
              AND NOT list_contains([{OFFICE_STOP_WORDS}], x)
              AND NOT regexp_matches(x, '^\\d+$')
          ),
          list_filter(
            string_split(lower(official_office_name_r), ' '),
            x -> len(x) > 1
              AND NOT list_contains([{OFFICE_STOP_WORDS}], x)
              AND NOT regexp_matches(x, '^\\d+$')
          )
        )
        OR gamma_office_type > 0
        OR gamma_ballotready_position_id > 0
      )
"""
