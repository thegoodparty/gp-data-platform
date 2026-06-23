"""Pure SQL builders for the unload step."""

from __future__ import annotations

from loader.people_api.schema import unload_sql


def test_select_exprs_orders_by_ddl_and_nulls_prisma_extras() -> None:
    ddl_cols = ["id", "LALVOTERID", "State", "Mailing_HHGender_Description"]
    extras = {"Mailing_HHGender_Description"}
    exprs = unload_sql.select_exprs(ddl_cols, extras)
    assert exprs == [
        "`id`",
        "`LALVOTERID`",
        "`State`",
        "NULL AS `Mailing_HHGender_Description`",
    ]


def test_unload_statement_shape() -> None:
    sql = unload_sql.unload_statement(
        mart_fqn="cat.dbt.m_people_api__voter",
        select_exprs=["`id`", "`State`"],
        state="FL",
        s3_dir="s3://b/voter_export_20260622/state=FL/",
    )
    assert "INSERT OVERWRITE DIRECTORY 's3://b/voter_export_20260622/state=FL/'" in sql
    assert "USING csv" in sql
    assert "'sep' = '\\t'" in sql and "'nullValue' = ''" in sql
    assert "'quote' = '\"'" in sql and "'escape' = '\"'" in sql and "'header' = 'false'" in sql
    assert "SELECT `id`, `State`" in sql
    assert "FROM cat.dbt.m_people_api__voter" in sql
    assert "WHERE `State` = 'FL'" in sql


def test_count_by_state_statement() -> None:
    sql = unload_sql.count_by_state_statement("cat.dbt.m_people_api__voter")
    assert sql == ("SELECT `State` AS state, count(*) AS n FROM cat.dbt.m_people_api__voter GROUP BY `State`")
