-- Per-run history of the published person id and slug: the close of an id's
-- row is the retirement signal, and the slug on that row is the URL it was last
-- published under. Neither can be rebuilt after the fact. Two columns in SQL
-- rather than a relation snapshot, so a type change elsewhere in the mart
-- cannot break the merge. The post_hook keeps 90 days of Delta history because
-- predictive optimization vacuums this schema on the default 7-day window.
{% snapshot snapshot__m_election_api__person %}
    {{
        config(
            unique_key="id",
            strategy="check",
            check_cols=["slug"],
            hard_deletes="invalidate",
            tags=["civics", "person", "snapshot"],
            post_hook="alter table {{ this }} set tblproperties ('delta.deletedFileRetentionDuration' = 'interval 90 days', 'delta.logRetentionDuration' = 'interval 90 days')",
        )
    }}
    select id, slug
    from {{ ref("m_election_api__person") }}
{% endsnapshot %}
