{% macro non_br_cluster_canonicals(cluster_id_expr) %}
    -- Salted UUIDs derived from cluster_id, distinct per grain.
    -- Used by int__civics_er_canonical_ids' non_br_cluster_matches branch
    -- so cluster members at every grain share the same canonical gp_*_id
    -- and collapse to one mart row.
    {{ generate_salted_uuid(fields=[cluster_id_expr], salt="non_br_candidacy_stage") }}
    as canonical_gp_candidacy_stage_id,
    {{ generate_salted_uuid(fields=[cluster_id_expr], salt="non_br_election_stage") }}
    as canonical_gp_election_stage_id,
    {{ generate_salted_uuid(fields=[cluster_id_expr], salt="non_br_candidacy") }}
    as canonical_gp_candidacy_id,
    {{ generate_salted_uuid(fields=[cluster_id_expr], salt="non_br_candidate") }}
    as canonical_gp_candidate_id,
    {{ generate_salted_uuid(fields=[cluster_id_expr], salt="non_br_election") }}
    as canonical_gp_election_id
{% endmacro %}
