{#
    Force is_win_icp / is_win_supersize_icp to false when the office's
    icp_win_effective_date is set and *every* relevant election date is null
    or strictly before that effective date. Any future election date falls
    through to the underlying icp_attribute.

    Pass `cast(null as date)` (or 'null') for a date column the caller doesn't
    have — null comparisons resolve to true via the coalesce, so absent
    columns don't affect the gate.

    Args:
      icp_attribute: SQL expression producing the underlying ICP boolean
        (e.g. 'icp.icp_office_win', or a coalesce with a fallback).
      primary_date, primary_runoff_date, general_date, general_runoff_date:
        SQL expressions producing the four relevant election dates.
      effective_date: SQL expression for the office's icp_win_effective_date.
#}
{% macro win_icp_date_gate(
    icp_attribute,
    primary_date,
    primary_runoff_date,
    general_date,
    general_runoff_date,
    effective_date
) %}
    case
        when
            {{ effective_date }} is not null
            and coalesce({{ primary_date }} < {{ effective_date }}, true)
            and coalesce({{ primary_runoff_date }} < {{ effective_date }}, true)
            and coalesce({{ general_date }} < {{ effective_date }}, true)
            and coalesce({{ general_runoff_date }} < {{ effective_date }}, true)
        then false
        else {{ icp_attribute }}
    end
{% endmacro %}
