{% macro non_br_cluster_canonicals(cluster_id_expr) %}
    -- Non-BR clusters: emit canonical IDs only at the candidacy and candidate
    -- grains (where Splink's entity-resolution clustering is the canonical
    -- merge mechanism). Higher-grain IDs (election_stage, election) are NULL
    -- here, letting TS/DDHQ int models fall back to their natural hashes —
    -- which align with BR's natural id when BR has the race independently.
    -- This avoids the cluster-derived-vs-BR-natural id conflict that would
    -- otherwise duplicate rows downstream (e.g., m_election_api joins on
    -- br_position_database_id). The candidacy_stage grain is also NULL
    -- because that mart uses cluster_id-based FOJ (PR #355), not
    -- canonical_id-based merging.
    cast(null as string) as canonical_gp_candidacy_stage_id,
    cast(null as string) as canonical_gp_election_stage_id,
    {{ generate_salted_uuid(fields=[cluster_id_expr], salt="non_br_candidacy") }}
    as canonical_gp_candidacy_id,
    {{ generate_salted_uuid(fields=[cluster_id_expr], salt="non_br_candidate") }}
    as canonical_gp_candidate_id,
    cast(null as string) as canonical_gp_election_id
{% endmacro %}
