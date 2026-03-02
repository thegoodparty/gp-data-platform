{% materialization sql, adapter = "databricks" %}

    {%- set target_relation = this -%}

    {% call statement("main") -%} {{ sql }} {%- endcall %}

    {{ return({"relations": []}) }}

{% endmaterialization %}
