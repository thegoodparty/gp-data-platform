from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, regexp_replace
from pyspark.sql.types import BooleanType, DoubleType, StringType

ELECTION_COLUMNS = [
    "General_2030",
    "Primary_2030",
    "OtherElection_2030",
    "AnyElection_2029",
    "General_2028",
    "Primary_2028",
    "PresidentialPrimary_2028",
    "OtherElection_2028",
    "AnyElection_2027",
    "General_2026",
    "Primary_2026",
    "OtherElection_2026",
    "AnyElection_2025",
    "General_2024",
    "Primary_2024",
    "PresidentialPrimary_2024",
    "OtherElection_2024",
    "AnyElection_2023",
    "General_2022",
    "Primary_2022",
    "OtherElection_2022",
    "AnyElection_2021",
    "General_2020",
    "Primary_2020",
    "PresidentialPrimary_2020",
    "OtherElection_2020",
    "AnyElection_2019",
    "General_2018",
    "Primary_2018",
    "OtherElection_2018",
    "AnyElection_2017",
    "General_2016",
    "Primary_2016",
    "PresidentialPrimary_2016",
    "OtherElection_2016",
    "AnyElection_2015",
    "General_2014",
    "Primary_2014",
    "OtherElection_2014",
    "AnyElection_2013",
    "General_2012",
    "Primary_2012",
    "PresidentialPrimary_2012",
    "OtherElection_2012",
    "AnyElection_2011",
    "General_2010",
    "Primary_2010",
    "OtherElection_2010",
    "AnyElection_2009",
    "General_2008",
    "Primary_2008",
    "PresidentialPrimary_2008",
    "OtherElection_2008",
    "AnyElection_2007",
    "General_2006",
    "Primary_2006",
    "OtherElection_2006",
    "AnyElection_2005",
    "General_2004",
    "Primary_2004",
    "PresidentialPrimary_2004",
    "OtherElection_2004",
    "AnyElection_2003",
    "General_2002",
    "Primary_2002",
    "OtherElection_2002",
    "AnyElection_2001",
    "General_2000",
    "Primary_2000",
    "PresidentialPrimary_2000",
    "OtherElection_2000",
]

PERFORMANCE_PERCENTAGE_COLUMNS = [
    "Voters_VotingPerformanceEvenYearGeneral",
    "Voters_VotingPerformanceEvenYearPrimary",
    "Voters_VotingPerformanceEvenYearGeneralAndPrimary",
    "Voters_VotingPerformanceMinorElection",
]


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model reads in the L2 UNIFORM data and unions them together.
    This is done in pyspark since in SparkSQL, there are some column data type mismatches which cannot
    be uncovered from SQL logs.
    """
    # configure the data model
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="LALVOTERID",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "l2", "nationwide_uniform", "uniform"],
    )

    # get the max loaded_at for the incremental run
    if dbt.is_incremental:
        this_df = session.table(f"{dbt.this}")

    # Read all the states and union them together. Since only literal values are allowed in `dbt.ref`,
    # we need to use a manual loop to read in each state

    # Alabama
    al_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_al_uniform").withColumn(
        "state_postal_code", lit("AL")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "AL")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        al_df = al_df.filter(col("loaded_at") > max_loaded_at)

    # Alaska
    ak_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ak_uniform").withColumn(
        "state_postal_code", lit("AK")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "AK")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ak_df = ak_df.filter(col("loaded_at") > max_loaded_at)

    # Arizona
    az_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_az_uniform").withColumn(
        "state_postal_code", lit("AZ")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "AZ")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        az_df = az_df.filter(col("loaded_at") > max_loaded_at)

    # Arkansas
    ar_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ar_uniform").withColumn(
        "state_postal_code", lit("AR")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "AR")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ar_df = ar_df.filter(col("loaded_at") > max_loaded_at)

    # California
    ca_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ca_uniform").withColumn(
        "state_postal_code", lit("CA")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "CA")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ca_df = ca_df.filter(col("loaded_at") > max_loaded_at)

    # Colorado
    co_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_co_uniform").withColumn(
        "state_postal_code", lit("CO")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "CO")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        co_df = co_df.filter(col("loaded_at") > max_loaded_at)

    # Connecticut
    ct_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ct_uniform").withColumn(
        "state_postal_code", lit("CT")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "CT")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ct_df = ct_df.filter(col("loaded_at") > max_loaded_at)

    # Delaware
    de_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_de_uniform").withColumn(
        "state_postal_code", lit("DE")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "DE")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        de_df = de_df.filter(col("loaded_at") > max_loaded_at)

    # District of Columbia
    dc_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_dc_uniform").withColumn(
        "state_postal_code", lit("DC")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "DC")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        dc_df = dc_df.filter(col("loaded_at") > max_loaded_at)

    # Florida
    fl_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_fl_uniform").withColumn(
        "state_postal_code", lit("FL")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "FL")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        fl_df = fl_df.filter(col("loaded_at") > max_loaded_at)

    # Georgia
    ga_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ga_uniform").withColumn(
        "state_postal_code", lit("GA")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "GA")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ga_df = ga_df.filter(col("loaded_at") > max_loaded_at)

    # Hawaii
    hi_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_hi_uniform").withColumn(
        "state_postal_code", lit("HI")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "HI")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        hi_df = hi_df.filter(col("loaded_at") > max_loaded_at)

    # Idaho
    id_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_id_uniform").withColumn(
        "state_postal_code", lit("ID")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "ID")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        id_df = id_df.filter(col("loaded_at") > max_loaded_at)

    # Illinois
    il_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_il_uniform").withColumn(
        "state_postal_code", lit("IL")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "IL")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        il_df = il_df.filter(col("loaded_at") > max_loaded_at)

    # Indiana
    in_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_in_uniform").withColumn(
        "state_postal_code", lit("IN")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "IN")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        in_df = in_df.filter(col("loaded_at") > max_loaded_at)

    # Iowa
    ia_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ia_uniform").withColumn(
        "state_postal_code", lit("IA")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "IA")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ia_df = ia_df.filter(col("loaded_at") > max_loaded_at)

    # Kansas
    ks_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ks_uniform").withColumn(
        "state_postal_code", lit("KS")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "KS")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ks_df = ks_df.filter(col("loaded_at") > max_loaded_at)

    # Kentucky
    ky_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ky_uniform").withColumn(
        "state_postal_code", lit("KY")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "KY")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ky_df = ky_df.filter(col("loaded_at") > max_loaded_at)

    # Louisiana
    la_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_la_uniform").withColumn(
        "state_postal_code", lit("LA")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "LA")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        la_df = la_df.filter(col("loaded_at") > max_loaded_at)

    # Maine
    me_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_me_uniform").withColumn(
        "state_postal_code", lit("ME")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "ME")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        me_df = me_df.filter(col("loaded_at") > max_loaded_at)

    # Maryland
    md_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_md_uniform").withColumn(
        "state_postal_code", lit("MD")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "MD")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        md_df = md_df.filter(col("loaded_at") > max_loaded_at)

    # Massachusetts
    ma_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ma_uniform").withColumn(
        "state_postal_code", lit("MA")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "MA")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ma_df = ma_df.filter(col("loaded_at") > max_loaded_at)

    # Michigan
    mi_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_mi_uniform").withColumn(
        "state_postal_code", lit("MI")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "MI")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        mi_df = mi_df.filter(col("loaded_at") > max_loaded_at)

    # Minnesota
    mn_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_mn_uniform").withColumn(
        "state_postal_code", lit("MN")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "MN")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        mn_df = mn_df.filter(col("loaded_at") > max_loaded_at)

    # Mississippi
    ms_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ms_uniform").withColumn(
        "state_postal_code", lit("MS")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "MS")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ms_df = ms_df.filter(col("loaded_at") > max_loaded_at)

    # Missouri
    mo_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_mo_uniform").withColumn(
        "state_postal_code", lit("MO")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "MO")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        mo_df = mo_df.filter(col("loaded_at") > max_loaded_at)

    # Montana
    mt_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_mt_uniform").withColumn(
        "state_postal_code", lit("MT")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "MT")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        mt_df = mt_df.filter(col("loaded_at") > max_loaded_at)

    # Nebraska
    ne_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ne_uniform").withColumn(
        "state_postal_code", lit("NE")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "NE")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ne_df = ne_df.filter(col("loaded_at") > max_loaded_at)

    # Nevada
    nv_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_nv_uniform").withColumn(
        "state_postal_code", lit("NV")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "NV")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        nv_df = nv_df.filter(col("loaded_at") > max_loaded_at)

    # New Hampshire
    nh_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_nh_uniform").withColumn(
        "state_postal_code", lit("NH")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "NH")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        nh_df = nh_df.filter(col("loaded_at") > max_loaded_at)

    # New Jersey
    nj_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_nj_uniform").withColumn(
        "state_postal_code", lit("NJ")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "NJ")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        nj_df = nj_df.filter(col("loaded_at") > max_loaded_at)

    # New Mexico
    nm_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_nm_uniform").withColumn(
        "state_postal_code", lit("NM")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "NM")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        nm_df = nm_df.filter(col("loaded_at") > max_loaded_at)

    # New York
    ny_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ny_uniform").withColumn(
        "state_postal_code", lit("NY")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "NY")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ny_df = ny_df.filter(col("loaded_at") > max_loaded_at)

    # North Carolina
    nc_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_nc_uniform").withColumn(
        "state_postal_code", lit("NC")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "NC")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        nc_df = nc_df.filter(col("loaded_at") > max_loaded_at)

    # North Dakota
    nd_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_nd_uniform").withColumn(
        "state_postal_code", lit("ND")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "ND")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        nd_df = nd_df.filter(col("loaded_at") > max_loaded_at)

    # Ohio
    oh_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_oh_uniform").withColumn(
        "state_postal_code", lit("OH")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "OH")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        oh_df = oh_df.filter(col("loaded_at") > max_loaded_at)

    # Oklahoma
    ok_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ok_uniform").withColumn(
        "state_postal_code", lit("OK")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "OK")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ok_df = ok_df.filter(col("loaded_at") > max_loaded_at)

    # Oregon
    or_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_or_uniform").withColumn(
        "state_postal_code", lit("OR")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "OR")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        or_df = or_df.filter(col("loaded_at") > max_loaded_at)

    # Pennsylvania
    pa_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_pa_uniform").withColumn(
        "state_postal_code", lit("PA")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "PA")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        pa_df = pa_df.filter(col("loaded_at") > max_loaded_at)

    # Rhode Island
    ri_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ri_uniform").withColumn(
        "state_postal_code", lit("RI")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "RI")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ri_df = ri_df.filter(col("loaded_at") > max_loaded_at)

    # South Carolina
    sc_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_sc_uniform").withColumn(
        "state_postal_code", lit("SC")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "SC")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        sc_df = sc_df.filter(col("loaded_at") > max_loaded_at)

    # South Dakota
    sd_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_sd_uniform").withColumn(
        "state_postal_code", lit("SD")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "SD")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        sd_df = sd_df.filter(col("loaded_at") > max_loaded_at)

    # Tennessee
    tn_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_tn_uniform").withColumn(
        "state_postal_code", lit("TN")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "TN")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        tn_df = tn_df.filter(col("loaded_at") > max_loaded_at)

    # Texas
    tx_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_tx_uniform").withColumn(
        "state_postal_code", lit("TX")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "TX")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        tx_df = tx_df.filter(col("loaded_at") > max_loaded_at)

    # Utah
    ut_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_ut_uniform").withColumn(
        "state_postal_code", lit("UT")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "UT")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        ut_df = ut_df.filter(col("loaded_at") > max_loaded_at)

    # Vermont
    vt_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_vt_uniform").withColumn(
        "state_postal_code", lit("VT")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "VT")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        vt_df = vt_df.filter(col("loaded_at") > max_loaded_at)

    # Virginia
    va_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_va_uniform").withColumn(
        "state_postal_code", lit("VA")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "VA")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        va_df = va_df.filter(col("loaded_at") > max_loaded_at)

    # Washington
    wa_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_wa_uniform").withColumn(
        "state_postal_code", lit("WA")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "WA")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        wa_df = wa_df.filter(col("loaded_at") > max_loaded_at)

    # West Virginia
    wv_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_wv_uniform").withColumn(
        "state_postal_code", lit("WV")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "WV")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        wv_df = wv_df.filter(col("loaded_at") > max_loaded_at)

    # Wisconsin
    wi_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_wi_uniform").withColumn(
        "state_postal_code", lit("WI")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "WI")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        wi_df = wi_df.filter(col("loaded_at") > max_loaded_at)

    # Wyoming
    wy_df: DataFrame = dbt.ref("stg_dbt_source__l2_s3_wy_uniform").withColumn(
        "state_postal_code", lit("WY")
    )
    if dbt.is_incremental:
        max_loaded_at = (
            this_df.filter(col("state_postal_code") == "WY")
            .agg({"loaded_at": "max"})
            .collect()[0][0]
        )
        wy_df = wy_df.filter(col("loaded_at") > max_loaded_at)

    # Use unionByName with allowMissingColumns=True to handle schema drift
    # when L2 delivers state updates with new columns in a staggered fashion
    df = (
        al_df.unionByName(ak_df, allowMissingColumns=True)
        .unionByName(az_df, allowMissingColumns=True)
        .unionByName(ar_df, allowMissingColumns=True)
        .unionByName(ca_df, allowMissingColumns=True)
        .unionByName(co_df, allowMissingColumns=True)
        .unionByName(ct_df, allowMissingColumns=True)
        .unionByName(de_df, allowMissingColumns=True)
        .unionByName(dc_df, allowMissingColumns=True)
        .unionByName(fl_df, allowMissingColumns=True)
        .unionByName(ga_df, allowMissingColumns=True)
        .unionByName(hi_df, allowMissingColumns=True)
        .unionByName(id_df, allowMissingColumns=True)
        .unionByName(il_df, allowMissingColumns=True)
        .unionByName(in_df, allowMissingColumns=True)
        .unionByName(ia_df, allowMissingColumns=True)
        .unionByName(ks_df, allowMissingColumns=True)
        .unionByName(ky_df, allowMissingColumns=True)
        .unionByName(la_df, allowMissingColumns=True)
        .unionByName(me_df, allowMissingColumns=True)
        .unionByName(md_df, allowMissingColumns=True)
        .unionByName(ma_df, allowMissingColumns=True)
        .unionByName(mi_df, allowMissingColumns=True)
        .unionByName(mn_df, allowMissingColumns=True)
        .unionByName(ms_df, allowMissingColumns=True)
        .unionByName(mo_df, allowMissingColumns=True)
        .unionByName(mt_df, allowMissingColumns=True)
        .unionByName(ne_df, allowMissingColumns=True)
        .unionByName(nv_df, allowMissingColumns=True)
        .unionByName(nh_df, allowMissingColumns=True)
        .unionByName(nj_df, allowMissingColumns=True)
        .unionByName(nm_df, allowMissingColumns=True)
        .unionByName(ny_df, allowMissingColumns=True)
        .unionByName(nc_df, allowMissingColumns=True)
        .unionByName(nd_df, allowMissingColumns=True)
        .unionByName(oh_df, allowMissingColumns=True)
        .unionByName(ok_df, allowMissingColumns=True)
        .unionByName(or_df, allowMissingColumns=True)
        .unionByName(pa_df, allowMissingColumns=True)
        .unionByName(ri_df, allowMissingColumns=True)
        .unionByName(sc_df, allowMissingColumns=True)
        .unionByName(sd_df, allowMissingColumns=True)
        .unionByName(tn_df, allowMissingColumns=True)
        .unionByName(tx_df, allowMissingColumns=True)
        .unionByName(ut_df, allowMissingColumns=True)
        .unionByName(vt_df, allowMissingColumns=True)
        .unionByName(va_df, allowMissingColumns=True)
        .unionByName(wa_df, allowMissingColumns=True)
        .unionByName(wv_df, allowMissingColumns=True)
        .unionByName(wi_df, allowMissingColumns=True)
        .unionByName(wy_df, allowMissingColumns=True)
    )

    # clean up columns with percentages
    for column in PERFORMANCE_PERCENTAGE_COLUMNS:
        df = df.withColumn(column, regexp_replace(col(column), "%", ""))
        df = df.withColumn(column, col(column).cast(DoubleType()))

    # cast elections as booleans
    for column in ELECTION_COLUMNS:
        df = df.withColumn(column, col(column).cast(BooleanType()))

    # other casting changes
    df = df.withColumn(
        "ConsumerData_PASS_Prospector_Home_Value_Mortgage_File",
        col("ConsumerData_PASS_Prospector_Home_Value_Mortgage_File").cast(StringType()),
    )

    # Extract state from LALVOTERID (substring starting at position 4 with length 2)
    df = df.withColumn("state_from_lalvoterid", col("LALVOTERID").substr(4, 2))

    if dbt.is_incremental:
        if df.count() == 0:
            return session.createDataFrame(data=[], schema=df.schema)

    return df
