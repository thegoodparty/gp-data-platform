{% macro _l2_uniform_preflight_emit(payload) %}
    {{ log("L2_PREFLIGHT|" ~ (payload | tojson), info=true) }}
{% endmacro %}


{% macro _l2_uniform_preflight_get_column_names(relation) %}
    {% set column_names = [] %}
    {% for column in adapter.get_columns_in_relation(relation) %}
        {% set normalized_name = column.name | lower %}
        {% if normalized_name not in column_names %}
            {% do column_names.append(normalized_name) %}
        {% endif %}
    {% endfor %}
    {{ return(column_names | sort) }}
{% endmacro %}


{% macro _l2_uniform_preflight_list_diff(left_columns, right_columns) %}
    {% set diff = [] %}
    {% for column_name in left_columns | sort %}
        {% if column_name not in right_columns %}
            {% do diff.append(column_name) %}
        {% endif %}
    {% endfor %}
    {{ return(diff) }}
{% endmacro %}


{% macro l2_uniform_schema_preflight(strict=true) %}
    {% set strict_mode = strict %}
    {% if strict_mode is none %} {% set strict_mode = true %}
    {% elif strict_mode is string %}
        {% set strict_mode = strict_mode | trim | lower in [
            "1",
            "t",
            "true",
            "y",
            "yes",
        ] %}
    {% endif %}

    {% set metadata_catalog = var(
        "preflight_metadata_catalog",
        (
            target.database
            if target.database is not none
            else "goodparty_data_catalog"
        ),
    ) %}
    {% set states = get_us_states_list(include_DC=true, include_US=false) %}
    {% set ns = namespace(source_found_count=0, stg_found_count=0, findings=[]) %}
    {% set all_source_columns = [] %}

    {% if states | length != 51 %}
        {{
            exceptions.raise_compiler_error(
                "L2 uniform preflight expected 51 jurisdictions (50 states + DC), got "
                ~ (states | length)
                ~ ". Check get_us_states_list(include_DC=true, include_US=false)."
            )
        }}
    {% endif %}

    {% do _l2_uniform_preflight_emit(
        {
            "kind": "config",
            "metadata_catalog": metadata_catalog,
            "strict": strict_mode,
            "states_expected": states | length,
        }
    ) %}

    {% for state in states %}
        {% set state_lower = state | lower %}
        {% set source_identifier = "l2_s3_" ~ state_lower ~ "_uniform" %}
        {% set stg_identifier = "stg_dbt_source__l2_s3_" ~ state_lower ~ "_uniform" %}

        {% set source_relation_name = (
            metadata_catalog ~ ".dbt_source." ~ source_identifier
        ) %}
        {% set stg_relation_name = metadata_catalog ~ ".dbt." ~ stg_identifier %}

        {% set source_relation = adapter.get_relation(
            database=metadata_catalog,
            schema="dbt_source",
            identifier=source_identifier,
        ) %}
        {% set stg_relation = adapter.get_relation(
            database=metadata_catalog, schema="dbt", identifier=stg_identifier
        ) %}

        {% if source_relation is none %}
            {% set finding = {
                "kind": "relation_missing",
                "scope": "state_source",
                "state": state,
                "relation": source_relation_name,
            } %}
            {% do ns.findings.append(finding) %}
            {% do _l2_uniform_preflight_emit(finding) %}
            {% set source_columns = [] %}
        {% else %}
            {% set ns.source_found_count = ns.source_found_count + 1 %}
            {% set source_columns = _l2_uniform_preflight_get_column_names(
                source_relation
            ) %}
            {% for source_column_name in source_columns %}
                {% if source_column_name not in all_source_columns %}
                    {% do all_source_columns.append(source_column_name) %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% if stg_relation is none %}
            {% set finding = {
                "kind": "relation_missing",
                "scope": "state_staging",
                "state": state,
                "relation": stg_relation_name,
            } %}
            {% do ns.findings.append(finding) %}
            {% do _l2_uniform_preflight_emit(finding) %}
            {% set stg_columns = [] %}
        {% else %}
            {% set ns.stg_found_count = ns.stg_found_count + 1 %}
            {% set stg_columns = _l2_uniform_preflight_get_column_names(stg_relation) %}
        {% endif %}

        {% if source_relation is not none and stg_relation is not none %}
            {% set stg_minus_src = _l2_uniform_preflight_list_diff(
                stg_columns, source_columns
            ) %}
            {% if stg_minus_src | length > 0 %}
                {% set finding = {
                    "kind": "stg_minus_src",
                    "state": state,
                    "source_relation": source_relation_name,
                    "staging_relation": stg_relation_name,
                    "columns": stg_minus_src,
                } %}
                {% do ns.findings.append(finding) %}
                {% do _l2_uniform_preflight_emit(finding) %}
            {% endif %}

            {% set src_minus_stg = _l2_uniform_preflight_list_diff(
                source_columns, stg_columns
            ) %}
            {% if src_minus_stg | length > 0 %}
                {% set finding = {
                    "kind": "src_minus_stg",
                    "state": state,
                    "source_relation": source_relation_name,
                    "staging_relation": stg_relation_name,
                    "columns": src_minus_stg,
                } %}
                {% do ns.findings.append(finding) %}
                {% do _l2_uniform_preflight_emit(finding) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% if ns.source_found_count == 0 or ns.stg_found_count == 0 %}
        {% set permission_msg %}
L2 uniform preflight could not read required metadata relations in catalog '{{ metadata_catalog }}'.
This usually indicates missing permissions or a wrong catalog.
Fallback: override catalog with --vars '{"preflight_metadata_catalog":"<catalog>"}', or request read access on {{ metadata_catalog }}.dbt_source and {{ metadata_catalog }}.dbt.
        {% endset %}
        {{ exceptions.raise_compiler_error(permission_msg | trim) }}
    {% endif %}

    {% set expected_primary_columns = [] %}
    {% for source_column_name in all_source_columns %}
        {% if source_column_name not in expected_primary_columns %}
            {% do expected_primary_columns.append(source_column_name) %}
        {% endif %}
    {% endfor %}
    {% for derived_column_name in ["state_postal_code", "state_from_lalvoterid"] %}
        {% if derived_column_name not in expected_primary_columns %}
            {% do expected_primary_columns.append(derived_column_name) %}
        {% endif %}
    {% endfor %}

    {% set primary_target_identifier = "int__l2_nationwide_uniform" %}
    {% set primary_target_relation_name = (
        metadata_catalog ~ ".dbt." ~ primary_target_identifier
    ) %}
    {% set primary_target_relation = adapter.get_relation(
        database=metadata_catalog,
        schema="dbt",
        identifier=primary_target_identifier,
    ) %}
    {% set primary_target_columns = [] %}

    {% if primary_target_relation is none %}
        {% set finding = {
            "kind": "relation_missing",
            "scope": "target_primary",
            "relation": primary_target_relation_name,
        } %}
        {% do ns.findings.append(finding) %}
        {% do _l2_uniform_preflight_emit(finding) %}
    {% else %}
        {% set primary_target_columns = _l2_uniform_preflight_get_column_names(
            primary_target_relation
        ) %}
        {% set primary_target_minus_src = _l2_uniform_preflight_list_diff(
            primary_target_columns, expected_primary_columns
        ) %}
        {% if primary_target_minus_src | length > 0 %}
            {% set finding = {
                "kind": "target_minus_src",
                "target_model": primary_target_identifier,
                "target_relation": primary_target_relation_name,
                "upstream_contract": "state_sources_plus_derived_columns",
                "columns": primary_target_minus_src,
            } %}
            {% do ns.findings.append(finding) %}
            {% do _l2_uniform_preflight_emit(finding) %}
        {% endif %}
    {% endif %}

    {% set downstream_contract_columns = [] %}
    {% for column_name in primary_target_columns %}
        {% if column_name not in downstream_contract_columns %}
            {% do downstream_contract_columns.append(column_name) %}
        {% endif %}
    {% endfor %}

    {% set flags_identifier = "int__l2_nationwide_haystaq_flags" %}
    {% set flags_relation_name = metadata_catalog ~ ".dbt." ~ flags_identifier %}
    {% set flags_relation = adapter.get_relation(
        database=metadata_catalog, schema="dbt", identifier=flags_identifier
    ) %}
    {% if flags_relation is none %}
        {% set finding = {
            "kind": "relation_missing",
            "scope": "upstream_haystaq_flags",
            "relation": flags_relation_name,
        } %}
        {% do ns.findings.append(finding) %}
        {% do _l2_uniform_preflight_emit(finding) %}
    {% else %}
        {% set flags_columns = _l2_uniform_preflight_get_column_names(flags_relation) %}
        {% for flags_column_name in flags_columns %}
            {% if flags_column_name[
                :3
            ] == "hf_" and flags_column_name not in downstream_contract_columns %}
                {% do downstream_contract_columns.append(flags_column_name) %}
            {% endif %}
        {% endfor %}
        {% if "haystaq_flags_loaded_at" not in downstream_contract_columns %}
            {% do downstream_contract_columns.append("haystaq_flags_loaded_at") %}
        {% endif %}
    {% endif %}

    {% set scores_identifier = "int__l2_nationwide_haystaq_scores" %}
    {% set scores_relation_name = metadata_catalog ~ ".dbt." ~ scores_identifier %}
    {% set scores_relation = adapter.get_relation(
        database=metadata_catalog, schema="dbt", identifier=scores_identifier
    ) %}
    {% if scores_relation is none %}
        {% set finding = {
            "kind": "relation_missing",
            "scope": "upstream_haystaq_scores",
            "relation": scores_relation_name,
        } %}
        {% do ns.findings.append(finding) %}
        {% do _l2_uniform_preflight_emit(finding) %}
    {% else %}
        {% set scores_columns = _l2_uniform_preflight_get_column_names(
            scores_relation
        ) %}
        {% for scores_column_name in scores_columns %}
            {% if scores_column_name[
                :3
            ] == "hs_" and scores_column_name not in downstream_contract_columns %}
                {% do downstream_contract_columns.append(scores_column_name) %}
            {% endif %}
        {% endfor %}
        {% if "haystaq_scores_loaded_at" not in downstream_contract_columns %}
            {% do downstream_contract_columns.append("haystaq_scores_loaded_at") %}
        {% endif %}
    {% endif %}

    {% set downstream_target_identifier = "int__l2_nationwide_uniform_w_haystaq" %}
    {% set downstream_target_relation_name = (
        metadata_catalog ~ ".dbt." ~ downstream_target_identifier
    ) %}
    {% set downstream_target_relation = adapter.get_relation(
        database=metadata_catalog,
        schema="dbt",
        identifier=downstream_target_identifier,
    ) %}

    {% if downstream_target_relation is none %}
        {% set finding = {
            "kind": "relation_missing",
            "scope": "target_downstream",
            "relation": downstream_target_relation_name,
        } %}
        {% do ns.findings.append(finding) %}
        {% do _l2_uniform_preflight_emit(finding) %}
    {% else %}
        {% set downstream_target_columns = _l2_uniform_preflight_get_column_names(
            downstream_target_relation
        ) %}
        {% set downstream_target_minus_src = _l2_uniform_preflight_list_diff(
            downstream_target_columns, downstream_contract_columns
        ) %}
        {% if downstream_target_minus_src | length > 0 %}
            {% set finding = {
                "kind": "target_minus_src",
                "target_model": downstream_target_identifier,
                "target_relation": downstream_target_relation_name,
                "upstream_contract": "uniform_plus_hf_hs_aliases",
                "columns": downstream_target_minus_src,
            } %}
            {% do ns.findings.append(finding) %}
            {% do _l2_uniform_preflight_emit(finding) %}
        {% endif %}
    {% endif %}

    {% if ns.findings | length == 0 %} {% set status = "clean" %}
    {% elif strict_mode %} {% set status = "fail" %}
    {% else %} {% set status = "warn" %}
    {% endif %}

    {% do _l2_uniform_preflight_emit(
        {
            "kind": "summary",
            "metadata_catalog": metadata_catalog,
            "strict": strict_mode,
            "status": status,
            "states_evaluated": states | length,
            "source_relations_found": ns.source_found_count,
            "staging_relations_found": ns.stg_found_count,
            "finding_count": ns.findings | length,
        }
    ) %}

    {% if ns.findings | length > 0 and strict_mode %}
        {{
            exceptions.raise_compiler_error(
                "L2 uniform schema preflight failed with "
                ~ (ns.findings | length)
                ~ " finding(s). See L2_PREFLIGHT| JSON lines for details."
            )
        }}
    {% endif %}

    {{
        return(
            {
                "finding_count": ns.findings | length,
                "strict": strict_mode,
                "status": status,
            }
        )
    }}
{% endmacro %}
