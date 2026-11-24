{#-
    Stamps `sensitive` on the raw-table columns the source YAML and the L2
    seed declare, so tag propagation has something to carry the masks down
    from. `dbt build` never alters a source relation, so the declarations would
    otherwise stay declarative forever.

    Prod only. `on-run-start` also fires for `dbt seed`, which is the prod job's
    first step, so the WHICH gate keeps the hook from running before the L2
    seed it reads exists. CI and dev targets never match, so they never touch a
    raw table.

    Reads the current tags once and writes only what differs, because steady
    state should be zero statements: `create or replace table` keeps a table's
    tags, so only a loader that drops and recreates leaves work behind.

    Never unsets. A declaration removed from the YAML leaves its tag in place:
    a stale tag over-masks, which fails safe, whereas guessing at provenance
    would let a tag that arrived by propagation be deleted.

    The seed is located through graph.nodes rather than ref(), because a ref()
    reached only from a hook leaves dbt unable to infer the operation's
    dependencies and parsing fails.

    The two candidate sets -- source declarations and L2 seed columns -- are
    built first, checked together, and only then written, so a failure on either
    one leaves nothing applied.
-#}
{% macro apply_source_sensitive_tags() %}

    {#- dbt renders an on-run-start hook once at parse time, where execute is
        false and the graph is an empty dict: no nodes, no sources. That render
        reaches the target gate below as a match, so without this guard the
        graph walk raises before any model runs and the whole build dies. CI's
        target hides it, since the gate returns early there. -#}
    {%- if not execute -%} {{ return("") }} {%- endif -%}

    {%- if target.name != "prod" or flags.WHICH not in ("run", "build") -%}
        {{ return("") }}
    {%- endif -%}

    {%- set source_pairs = declared_source_sensitive_tags() -%}
    {%- set l2_pairs = declared_l2_sensitive_tags() -%}
    {%- set expected = {} -%}
    {%- for pairs in [source_pairs, l2_pairs] -%}
        {%- for key, declared in pairs.items() -%}
            {%- do expected.update({key: declared}) -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- if not expected -%} {{ return("") }} {%- endif -%}

    {#- Every check runs before any ALTER, so a stale declaration writes nothing
        and the failure message is true on both paths. -#}
    {{ assert_sensitive_tag_targets_exist(expected, existing_source_columns()) }}

    {%- set current = current_sensitive_tags() -%}
    {%- set written = [] -%}
    {%- for key, declared in expected.items() -%}
        {%- if current.get(key) != declared["value"] -%}
            {{ set_sensitive_tag(key[0], key[1], key[2], key[3], declared["value"]) }}
            {%- do written.append(key) -%}
        {%- endif -%}
    {%- endfor -%}

    {{
        log(
            "sensitive tag apply: " ~ written
            | length ~ " written of " ~ expected
            | length ~ " declared pairs (" ~ source_pairs
            | length ~ " source, " ~ l2_pairs
            | length ~ " L2)",
            info=True,
        )
    }}

    {{ return("") }}
{% endmacro %}


{#-
    Every (catalog, schema) whose raw tables this hook may tag. The database is
    part of the key and part of the filter: the source YAML also declares
    tagged columns in the federated gp_api_pg catalog and in segment_storage,
    and only the catalogs listed here may ever be written. Naming catalog and
    schema together means a stray declaration cannot widen this.
-#}
{% macro sensitive_tag_scope() %}
    {%- set scope = [
        ("goodparty_data_catalog", "airbyte_source"),
        ("goodparty_data_catalog", "airbyte_source_finance"),
        ("goodparty_data_catalog", "er_source"),
        ("goodparty_data_catalog", "archives"),
        ("goodparty_data_catalog", "source_dsar"),
        ("goodparty_data_catalog", "dbt_source"),
        ("segment_storage", "gp_api"),
        ("segment_storage", "web_app"),
        ("raw", "amplitude_ingestion"),
    ] -%}
    {{ return(scope) }}
{% endmacro %}


{#-
    A SQL literal list of catalog.schema for an information_schema filter, e.g.
    ('goodparty_data_catalog', 'airbyte_source'), ...
-#}
{% macro sensitive_tag_scope_values() %}
    {%- set values = [] -%}
    {%- for db, schema in sensitive_tag_scope() -%}
        {%- do values.append("('" ~ db ~ "', '" ~ schema ~ "')") -%}
    {%- endfor -%}
    {{ return(values | join(", ")) }}
{% endmacro %}


{#-
    Raises with the exact pair, the file that declares it and the value, so the
    fix is a one-line YAML edit without a second query.

    Failing the whole build is deliberate. The alternative -- warn and skip the
    missing pairs -- would apply the rest and leave a declaration silently
    unapplied, so a column could read unmasked while the source still says it is
    sensitive. A build that stops leaves yesterday's data in place, which is
    recoverable; a mask that is quietly missing is not. A missing column is a
    declaration that has drifted from the loader, and that is a change worth
    stopping for.

    Both candidate sets are checked together before the first ALTER, so this
    message is true whichever pair is missing.
-#}
{% macro assert_sensitive_tag_targets_exist(expected, existing) %}
    {%- for key, declared in expected.items() -%}
        {%- if key not in existing -%}
            {%- set parts = key -%}
            {{
                exceptions.raise_compiler_error(
                    "sensitive tag apply: a declared source column is missing in prod.\n"
                    ~ "  "
                    ~ parts[0]
                    ~ "."
                    ~ parts[1]
                    ~ "."
                    ~ parts[2]
                    ~ "."
                    ~ parts[3]
                    ~ "\n"
                    ~ "  declared in "
                    ~ declared["file"]
                    ~ " (sensitive: "
                    ~ declared["value"]
                    ~ ")\n"
                    ~ "Nothing was applied. If the column was renamed, update the source YAML "
                    ~ "in the same change. If the source has not landed in prod yet, the "
                    ~ "declaration is ahead of the loader."
                )
            }}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}


{#- Existing (catalog, schema, table, column) tuples in scope, read once. -#}
{% macro existing_source_columns() %}
    {%- set rows = run_query(
        "select table_catalog, table_schema, table_name, column_name "
        ~ "from system.information_schema.columns "
        ~ "where (table_catalog, table_schema) in ("
        ~ sensitive_tag_scope_values()
        ~ ")"
    ) -%}
    {%- set existing = {} -%}
    {%- for row in rows.rows -%}
        {%- do existing.update({(row[0], row[1], row[2], row[3]): true}) -%}
    {%- endfor -%}
    {{ return(existing) }}
{% endmacro %}


{#- Current sensitive value per (catalog, schema, table, column), read once. -#}
{% macro current_sensitive_tags() %}
    {%- set rows = run_query(
        "select catalog_name, schema_name, table_name, column_name, tag_value "
        ~ "from system.information_schema.column_tags "
        ~ "where tag_name = 'sensitive' "
        ~ "and (catalog_name, schema_name) in ("
        ~ sensitive_tag_scope_values()
        ~ ")"
    ) -%}
    {%- set current = {} -%}
    {%- for row in rows.rows -%}
        {%- do current.update({(row[0], row[1], row[2], row[3]): row[4]}) -%}
    {%- endfor -%}
    {{ return(current) }}
{% endmacro %}


{#- Write one tag, or nothing when it already holds the declared value. -#}
{% macro set_sensitive_tag(catalog, schema, relation, column, value) %}
    {%- do run_query(
        "alter table `"
        ~ catalog
        ~ "`.`"
        ~ schema
        ~ "`.`"
        ~ relation
        ~ "` "
        ~ "alter column `"
        ~ column
        ~ "` set tags ('sensitive' = '"
        ~ value
        ~ "')"
    ) -%}
{% endmacro %}


{#- The declared source columns, from the manifest's source column meta. -#}
{% macro declared_source_sensitive_tags() %}
    {%- set scope = sensitive_tag_scope() -%}
    {%- set expected = {} -%}
    {%- for node in graph.sources.values() -%}
        {%- if (node.database, node.schema) in scope -%}
            {%- for column_name, column in (node.columns or {}).items() -%}
                {%- set meta = (column.config or {}).get("meta") or {} -%}
                {%- set value = meta.get("sensitive") -%}
                {%- if value -%}
                    {%- do expected.update(
                        {
                            (
                                node.database,
                                node.schema,
                                node.name,
                                column_name,
                            ): {
                                "value": value,
                                "file": node.original_file_path,
                            }
                        }
                    ) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(expected) }}
{% endmacro %}


{#-
    The L2 seed's columns on the raw voter-file tables. Only the `_uniform`
    tables carry them: the `_vote_history` tables have none of the 33 columns,
    so they are not candidates. The twice-daily job builds no models from these
    tables, but the hook walks the graph and tags the raw relations regardless.
-#}
{#-
    Resolved from the manifest rather than through ref(), because a ref() this
    deep inside a hook leaves dbt unable to infer the operation's dependencies
    and it fails parsing. The seed's schema follows the target, so the location
    has to be read from the parsed node rather than hardcoded.
-#}
{% macro l2_sensitive_tags_relation() %}
    {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == "seed" and node.name == "l2_sensitive_tags" -%}
            {{
                return(
                    "`"
                    ~ node.database
                    ~ "`.`"
                    ~ node.schema
                    ~ "`.`"
                    ~ node.alias
                    ~ "`"
                )
            }}
        {%- endif -%}
    {%- endfor -%}
    {{ return(none) }}
{% endmacro %}


{% macro declared_l2_sensitive_tags() %}
    {%- set seed_relation = l2_sensitive_tags_relation() -%}
    {%- if seed_relation is none -%} {{ return({}) }} {%- endif -%}
    {%- set seed_tags = {} -%}
    {%- if execute -%}
        {%- set results = run_query(
            "select column_name, sensitive from "
            ~ seed_relation
            ~ " order by column_name"
        ) -%}
        {%- for row in results.rows -%}
            {%- do seed_tags.update({row[0]: row[1]}) -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if not seed_tags -%} {{ return({}) }} {%- endif -%}

    {%- set expected = {} -%}
    {%- for node in graph.sources.values() -%}
        {%- if node.database == "goodparty_data_catalog" and node.schema == "dbt_source" and node.name.startswith(
            "l2_s3_"
        ) and node.name.endswith(
            "_uniform"
        ) -%}
            {%- for column_name, value in seed_tags.items() -%}
                {%- do expected.update(
                    {
                        (
                            node.database,
                            node.schema,
                            node.name,
                            column_name,
                        ): {
                            "value": value,
                            "file": "seeds/l2_sensitive_tags.csv",
                        }
                    }
                ) -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(expected) }}
{% endmacro %}
