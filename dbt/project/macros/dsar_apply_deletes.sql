{% macro dsar_delete_targets() %}
    {#-
        Every warehouse copy that a suppressed identifier must be removed from, with
        the raw-column expression that puts it into the register's shape.

        Each entry mirrors a staging filter. The staging models hide these rows from
        everything downstream; this list is what physically removes them from the
        copies that re-ingest on their own schedule (Airbyte landing tables, the
        insert-only airbyte_internal raw JSON) or that never re-ingest but never
        forget either (dbt snapshots). Keep the two in step: a filter added to a
        staging model needs its raw expression added here.

        `match` is "scalar" when the expression yields one identifier, or "array"
        when it yields an array of them (BallotReady contacts).
    -#}
    {%- set contacts_emails = (
        "transform(from_json(regexp_replace(replace(replace({col}, '=>nil,', '=>null,'), '=>nil}', '=>null}'), '=>', ':'),"
        ~ " 'ARRAY<STRUCT<email: STRING, phone: STRING, type: STRING>>'), c -> lower(trim(c.email)))"
    ) -%}
    {%- set contacts_phones = (
        "transform(from_json(regexp_replace(replace(replace({col}, '=>nil,', '=>null,'), '=>nil}', '=>null}'), '=>', ':'),"
        ~ " 'ARRAY<STRUCT<email: STRING, phone: STRING, type: STRING>>'), c -> regexp_replace(c.phone, '[^0-9]', ''))"
    ) -%}
    {%- set raw = "get_json_object(_airbyte_data, '{path}')" -%}

    {{
        return(
            [
                {
                    "relation": source("airbyte_source", "gp_api_db_user"),
                    "predicates": [
                        {"expr": "email", "type": "email", "match": "scalar"},
                        {"expr": "phone", "type": "phone", "match": "scalar"},
                    ],
                },
                {
                    "relation": source("airbyte_source", "hubspot_api_contacts"),
                    "predicates": [
                        {
                            "expr": "get_json_object(properties, '$.email')",
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": "get_json_object(properties, '$.phone')",
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": source("airbyte_source", "hubspot_api_companies"),
                    "predicates": [
                        {
                            "expr": "properties_candidate_email",
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": "properties_phone",
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": source(
                        "airbyte_source", "ballotready_s3_candidacies_v3"
                    ),
                    "predicates": [
                        {"expr": "email", "type": "email", "match": "scalar"},
                        {"expr": "phone", "type": "phone", "match": "scalar"},
                    ],
                },
                {
                    "relation": source(
                        "airbyte_source", "ballotready_s3_office_holders_v3"
                    ),
                    "predicates": [
                        {
                            "expr": contacts_emails.replace("{col}", "contacts"),
                            "type": "email",
                            "match": "array",
                        },
                        {
                            "expr": contacts_phones.replace("{col}", "contacts"),
                            "type": "phone",
                            "match": "array",
                        },
                    ],
                },
                {
                    "relation": source(
                        "airbyte_source", "techspeed_gdrive_candidates"
                    ),
                    "predicates": [
                        {"expr": "email", "type": "email", "match": "scalar"},
                        {"expr": "phone", "type": "phone", "match": "scalar"},
                        {"expr": "phone_clean", "type": "phone", "match": "scalar"},
                    ],
                },
                {
                    "relation": source(
                        "airbyte_source", "techspeed_gdrive_officeholders"
                    ),
                    "predicates": [
                        {"expr": "email", "type": "email", "match": "scalar"},
                        {"expr": "phone", "type": "phone", "match": "scalar"},
                    ],
                },
                {
                    "relation": source(
                        "airbyte_source", "ddhq_gdrive_election_results"
                    ),
                    "predicates": [
                        {
                            "expr": "cast(cast(nullif(candidate_id, '') as float) as int)",
                            "type": "ddhq_candidate_id",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": source("airbyte_source", "amplitude_api_events"),
                    "predicates": [
                        {
                            "expr": "user_id",
                            "type": "gp_api_user_id",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("gp_api_db_user"),
                    "predicates": [
                        {
                            "expr": raw.replace("{path}", "$.email"),
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": raw.replace("{path}", "$.phone"),
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("hubspot_api_contacts"),
                    "predicates": [
                        {
                            "expr": raw.replace("{path}", "$.properties.email"),
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": raw.replace("{path}", "$.properties.phone"),
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("hubspot_api_companies"),
                    "predicates": [
                        {
                            "expr": raw.replace(
                                "{path}", "$.properties.candidate_email"
                            ),
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": raw.replace("{path}", "$.properties.phone"),
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("ballotready_s3_candidacies_v3"),
                    "predicates": [
                        {
                            "expr": raw.replace("{path}", "$.email"),
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": raw.replace("{path}", "$.phone"),
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("ballotready_s3_office_holders_v3"),
                    "predicates": [
                        {
                            "expr": contacts_emails.replace(
                                "{col}", raw.replace("{path}", "$.contacts")
                            ),
                            "type": "email",
                            "match": "array",
                        },
                        {
                            "expr": contacts_phones.replace(
                                "{col}", raw.replace("{path}", "$.contacts")
                            ),
                            "type": "phone",
                            "match": "array",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("techspeed_gdrive_candidates"),
                    "predicates": [
                        {
                            "expr": raw.replace("{path}", "$.email"),
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": raw.replace("{path}", "$.phone"),
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("techspeed_gdrive_officeholders"),
                    "predicates": [
                        {
                            "expr": raw.replace("{path}", "$.email"),
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": raw.replace("{path}", "$.phone"),
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("ddhq_gdrive_election_results"),
                    "predicates": [
                        {
                            "expr": "cast(cast(nullif("
                            ~ raw.replace("{path}", "$.candidate_id")
                            ~ ", '') as float) as int)",
                            "type": "ddhq_candidate_id",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_raw_stream("amplitude_api_events"),
                    "predicates": [
                        {
                            "expr": raw.replace("{path}", "$.user_id"),
                            "type": "gp_api_user_id",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_snapshot("snapshot__hubspot_api_contacts"),
                    "predicates": [
                        {
                            "expr": "properties_email",
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": "properties_phone",
                            "type": "phone",
                            "match": "scalar",
                        },
                        {
                            "expr": "properties_mobilephone",
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
                {
                    "relation": dsar_snapshot("snapshot__hubspot_api_companies"),
                    "predicates": [
                        {
                            "expr": "properties_candidate_email",
                            "type": "email",
                            "match": "scalar",
                        },
                        {
                            "expr": "properties_phone",
                            "type": "phone",
                            "match": "scalar",
                        },
                    ],
                },
            ]
        )
    }}
{% endmacro %}


{% macro dsar_raw_stream(stream) %}
    {#- The insert-only Airbyte raw table behind an airbyte_source landing table. -#}
    {{
        return(
            api.Relation.create(
                database=target.database,
                schema="airbyte_internal",
                identifier="airbyte_source_raw__stream_" ~ stream,
            )
        )
    }}
{% endmacro %}


{% macro dsar_snapshot(name) %}
    {#-
        A production dbt snapshot. Named explicitly rather than via ref() because a
        dev target would resolve ref() to the developer's schema, where the snapshot
        does not exist, while the copy that retains history lives only in prod.
    -#}
    {{
        return(
            api.Relation.create(
                database=target.database, schema="dbt", identifier=name
            )
        )
    }}
{% endmacro %}


{% macro dsar_apply_deletes(dry_run=true, request_id=none) %}
    {#-
        Remove every row matching the DSAR suppression register from the warehouse
        copies that the staging filters cannot reach: Airbyte landing tables, their
        insert-only raw JSON, and the dbt snapshots.

            dbt run-operation dsar_apply_deletes                                   -- counts only
            dbt run-operation dsar_apply_deletes --args '{dry_run: false}'         -- delete
            dbt run-operation dsar_apply_deletes --args '{request_id: DATA-XXXX}'  -- one request

        Who runs it decides whether it can delete. From the dbt Cloud CLI it runs
        with your own Databricks credentials, which can count but hold no MODIFY on
        the raw sources, so the CLI is for dry runs. Deletes go through the dbt Cloud
        job in the Prod deployment environment, which runs as the dbt Cloud service
        principal; the skill's run_deletes.py triggers it with these arguments as a
        step override. Idempotent, so the job's scheduled unscoped run is the
        standing control against sources that re-ingest a deleted person.

        Deletes are logical. No purge, no vacuum; the legal review settled that a
        Delta DELETE meets the statute.

        The default is a dry run. The targets are production tables whatever the
        dbt target, so nothing here trusts target.name; the only way to delete is to
        say so.
    -#}
    {%- if not execute -%} {{ return(none) }} {%- endif -%}

    {%- set register = source("source_dsar", "suppressed_identifiers") -%}
    {%- set scope = "" -%}
    {%- if request_id -%}
        {%- set scope = " and r.request_id = '" ~ request_id ~ "'" -%}
    {%- endif -%}

    {%- set register_count = (
        run_query(
            "select count(*) from "
            ~ register
            ~ " r where r.identifier_value is not null"
            ~ scope
        )
        .columns[0]
        .values()[0]
    ) -%}
    {%- if register_count == 0 -%}
        {{
            log(
                "dsar_apply_deletes: register is empty for this scope, nothing to do.",
                info=true,
            )
        }}
        {{ return(none) }}
    {%- endif -%}

    {{
        log(
            "dsar_apply_deletes: "
            ~ ("DRY RUN, counting only" if dry_run else "DELETING")
            ~ " against "
            ~ register_count
            ~ " register row(s)"
            ~ (" for " ~ request_id if request_id else "")
            ~ ".",
            info=true,
        )
    }}

    {%- set summary = [] -%}
    {%- for t in dsar_delete_targets() -%}
        {%- set clauses = [] -%}
        {%- for p in t.predicates -%}
            {%- if p.match == "array" -%}
                {%- set hit = "array_contains(" ~ p.expr ~ ", r.identifier_value)" -%}
            {%- else -%}
                {%- set hit = (
                    dsar_normalize(p.expr, p.type) ~ " = r.identifier_value"
                ) -%}
            {%- endif -%}
            {%- do clauses.append(
                "exists (select 1 from "
                ~ register
                ~ " r where r.identifier_type = '"
                ~ p.type
                ~ "' and r.identifier_value is not null"
                ~ scope
                ~ " and "
                ~ hit
                ~ ")"
            ) -%}
        {%- endfor -%}
        {%- set where = clauses | join("\n    or ") -%}

        {%- set n = (
            run_query(
                "select count(*) from " ~ t.relation ~ " where " ~ where
            )
            .columns[0]
            .values()[0]
        ) -%}
        {%- if not dry_run and n > 0 -%}
            {%- do run_query("delete from " ~ t.relation ~ " where " ~ where) -%}
        {%- endif -%}
        {%- do summary.append({"relation": t.relation | string, "rows": n}) -%}
        {{ log("  " ~ ("%6d" | format(n)) ~ "  " ~ t.relation, info=true) }}
    {%- endfor -%}

    {%- set total = summary | sum(attribute="rows") -%}
    {{
        log(
            "dsar_apply_deletes: "
            ~ total
            ~ " row(s) across "
            ~ (summary | length)
            ~ " table(s) "
            ~ (
                "would be deleted. Re-run with --args '{dry_run: false}' to apply."
                if dry_run
                else "deleted."
            ),
            info=true,
        )
    }}
    {{ return(summary) }}
{% endmacro %}
