-- Every Segment staging model carries a person's gp-api user id. A new one copied
-- from a neighbour without the filter would pass suppressed people through, so this
-- fails at compile time naming the model.
{%- set unfiltered = [] %}
{%- if execute %}
    {%- for node in graph.nodes.values() %}
        {%- if node.resource_type == "model" and "segment_storage_source" in node.fqn %}
            {%- if "dsar_not_suppressed" not in node.raw_code and "segment_staging" not in node.raw_code %}
                {%- do unfiltered.append(node.name) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
{%- endif %}

{%- if unfiltered %}
    {{
        exceptions.raise_compiler_error(
            "Segment staging models without a DSAR filter: " ~ unfiltered
            | join(", ")
        )
    }}
{%- endif %}

select 1 as unfiltered_model
where false
