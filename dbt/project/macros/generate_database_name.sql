{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {#
      dbt reads one catalog and writes another. raw holds every loader's
      output; prod, the catalog named in the profile, holds everything dbt
      builds; dev holds personal and CI schemas. A model that names its own
      `database` keeps it, which is how a source in segment_storage stays there
      on every target.

      Only the prod target writes to the prod catalog. Every other target,
      including the named dev targets dbt Cloud gives each person, writes to
      dev, so a development build cannot materialize a staging layer full of
      sensitive columns next to the marts.

      Deferral still works: a dev run reads prod for the models it does not
      build, because the reflection comes from the defer environment rather
      than from this macro.
    #}
    {%- if custom_database_name is not none -%} {{ custom_database_name }}
    {%- elif target.name == "prod" -%} goodparty_data_catalog
    {%- else -%} dev
    {%- endif -%}
{%- endmacro %}
