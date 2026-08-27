{% macro min_label_propagation(adjacency_cte, nodes_cte, passes) %}
    /*
    Connected components via min-label propagation over a fixed number of
    unrolled passes (Spark SQL, no recursion). Emits the labels_0..labels_N CTE
    chain, each followed by a comma; the caller supplies the adjacency CTE
    (undirected src/dst pairs WITH a self-loop per node, so each pass takes
    min() over neighbors only and references the prior pass exactly once) and a
    nodes CTE with a record_key column, then selects from labels_{{ passes }}
    (and labels_{{ passes - 1 }} for the convergence proof).
    */
    labels_0 as (
        select record_key, record_key as person_group_key from {{ nodes_cte }}
    ),
    {% for n in range(1, passes + 1) %}
        labels_{{ n }} as (
            select a.src as record_key, min(l.person_group_key) as person_group_key
            from {{ adjacency_cte }} as a
            inner join labels_{{ n - 1 }} as l on l.record_key = a.dst
            group by a.src
        ){{ "," if not loop.last }}
    {% endfor %}
{% endmacro %}
