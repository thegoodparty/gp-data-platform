{#-
    DATA-1903: assert every issue in the haystaq_issue_tags seed appears at
    least once in m_election_api__district_top_issues.

    The mart's compile-time issue column list lives inline in the model SQL
    (a fresh-CI compile against an unmaterialized seed can't read it via
    dbt_utils.get_column_values). The seed is the runtime source for labels +
    flags. This test catches the silent failure mode where the seed gets a
    new issue but the mart's inline list doesn't, which would cause that
    issue's column to be missing from the AVG/UNPIVOT clauses and produce no
    mart rows for it.

    Returns rows iff the lists are out of sync.
-#}
select t.issue
from {{ ref("haystaq_issue_tags") }} as t
where
    t.issue not in (
        select distinct m.issue
        from {{ ref("m_election_api__district_top_issues") }} as m
        where m.issue is not null
    )
