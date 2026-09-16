-- Per-run history of the published person id and slug, so a retired id's
-- forwarding row (m_election_api__person_merge) can carry the slug as it was
-- last published and the run in which the id left Person. Neither can be
-- reconstructed after the fact: the slug depends on the name at the time, and
-- the id is simply gone from the next build. check strategy on slug, since a
-- slug changes only with the name or the id; hard_deletes closes the row of an
-- id that stopped being published, which is the retirement signal.
--
-- Defined in SQL rather than YAML so the snapshot carries only these two
-- columns. A relation snapshot of the mart would freeze thirty columns at first
-- capture, and a type change on any of them would break the merge.
{% snapshot snapshot__m_election_api__person %}
    {{
        config(
            unique_key="id",
            strategy="check",
            check_cols=["slug"],
            hard_deletes="invalidate",
            tags=["civics", "person", "snapshot"],
        )
    }}
    select id, slug
    from {{ ref("m_election_api__person") }}
{% endsnapshot %}
