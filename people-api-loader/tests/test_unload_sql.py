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
        "CAST(NULL AS STRING) AS `Mailing_HHGender_Description`",
    ]


def test_select_exprs_renames_mart_column_to_serving_name() -> None:
    # DistrictVoter: serving "State" is projected from the mart's lowercase `state`.
    ddl_cols = ["district_id", "voter_id", "State"]
    exprs = unload_sql.select_exprs(ddl_cols, set(), renames={"State": "state"})
    assert exprs == ["`district_id`", "`voter_id`", "`state` AS `State`"]


def test_unload_statement_partition_col_override() -> None:
    # DistrictVoter's mart partition column is lowercase `state`; the WHERE must use it.
    sql = unload_sql.unload_statement(
        mart_fqn="cat.dbt.m_people_api__districtvoter",
        select_exprs=["`district_id`", "`state` AS `State`"],
        state="FL",
        s3_dir="s3://b/x/DistrictVoter/state=FL/",
        partition_col="state",
    )
    assert "WHERE `state` = 'FL'" in sql


def test_count_by_state_statement_partition_col_override() -> None:
    sql = unload_sql.count_by_state_statement("cat.dbt.m_people_api__districtvoter", "state")
    assert sql == (
        "SELECT `state` AS state, count(*) AS n FROM cat.dbt.m_people_api__districtvoter GROUP BY `state`"
    )


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


def test_flat_unload_has_no_state_where() -> None:
    sql = unload_sql.unload_statement_flat(
        mart_fqn="cat.s.m", select_exprs=["`a`", "`b`"], s3_dir="s3://x/District/data/"
    )
    assert "WHERE" not in sql
    assert "INSERT OVERWRITE DIRECTORY 's3://x/District/data/'" in sql


def test_flat_count_statement() -> None:
    assert unload_sql.count_all_statement("cat.s.m") == "SELECT count(*) AS n FROM cat.s.m"


def test_select_exprs_transform_renames_buckets() -> None:
    # DistrictStats: rename two struct fields inside buckets on the way out, to_json'd.
    exprs = unload_sql.select_exprs(
        ["district_id", "buckets"],
        extra_columns=set(),
        transforms={"buckets": unload_sql.BUCKETS_TO_JSON_EXPR},
    )
    assert exprs[0] == "`district_id`"
    assert "to_json" in exprs[1].lower()
    assert "presenceOfChildren" in exprs[1]
    assert "estimatedIncomeRange" in exprs[1]
    assert exprs[1].endswith("AS `buckets`")
