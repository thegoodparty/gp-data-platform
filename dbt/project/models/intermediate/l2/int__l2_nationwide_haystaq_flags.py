import re
from typing import List, Optional, Set

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType


def _parse_state_allowlist(raw: Optional[str]) -> Optional[Set[str]]:
    if raw is None:
        return None
    normalized = raw.strip().upper()
    if not normalized:
        return None
    parts = re.split(r"[,\s]+", normalized)
    allowlist = {p for p in parts if p}
    return allowlist or None


def _cast_flag_columns(df: DataFrame) -> DataFrame:
    df = df.withColumn("LALVOTERID", col("LALVOTERID").cast(StringType()))
    for column_name in df.columns:
        if column_name.startswith("hf_"):
            df = df.withColumn(column_name, col(column_name).cast(StringType()))
    return df


def model(dbt, session: SparkSession) -> DataFrame:
    """
    Union all per-state Haystaq flags tables into a nationwide table.

    This intentionally mirrors `int__l2_nationwide_uniform.py`:
    - explicit refs per state (literal `dbt.ref(...)`)
    - per-state incremental filtering by `loaded_at`
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="LALVOTERID",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "l2", "nationwide_haystaq", "haystaq", "flags"],
    )

    state_allowlist = _parse_state_allowlist(dbt.config.get("l2_state_allowlist"))

    this_df: Optional[DataFrame] = None
    if dbt.is_incremental:
        this_df = session.table(f"{dbt.this}")

    per_state_dfs: List[DataFrame] = []

    # Alabama
    if state_allowlist is None or "AL" in state_allowlist:
        al_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_al_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("AL"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "AL")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                al_df = al_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(al_df))

    # Alaska
    if state_allowlist is None or "AK" in state_allowlist:
        ak_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ak_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("AK"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "AK")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ak_df = ak_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ak_df))

    # Arizona
    if state_allowlist is None or "AZ" in state_allowlist:
        az_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_az_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("AZ"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "AZ")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                az_df = az_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(az_df))

    # Arkansas
    if state_allowlist is None or "AR" in state_allowlist:
        ar_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ar_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("AR"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "AR")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ar_df = ar_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ar_df))

    # California
    if state_allowlist is None or "CA" in state_allowlist:
        ca_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ca_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("CA"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "CA")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ca_df = ca_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ca_df))

    # Colorado
    if state_allowlist is None or "CO" in state_allowlist:
        co_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_co_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("CO"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "CO")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                co_df = co_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(co_df))

    # Connecticut
    if state_allowlist is None or "CT" in state_allowlist:
        ct_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ct_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("CT"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "CT")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ct_df = ct_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ct_df))

    # Delaware
    if state_allowlist is None or "DE" in state_allowlist:
        de_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_de_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("DE"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "DE")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                de_df = de_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(de_df))

    # Washington DC
    if state_allowlist is None or "DC" in state_allowlist:
        dc_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_dc_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("DC"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "DC")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                dc_df = dc_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(dc_df))

    # Florida
    if state_allowlist is None or "FL" in state_allowlist:
        fl_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_fl_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("FL"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "FL")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                fl_df = fl_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(fl_df))

    # Georgia
    if state_allowlist is None or "GA" in state_allowlist:
        ga_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ga_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("GA"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "GA")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ga_df = ga_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ga_df))

    # Hawaii
    if state_allowlist is None or "HI" in state_allowlist:
        hi_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_hi_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("HI"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "HI")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                hi_df = hi_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(hi_df))

    # Idaho
    if state_allowlist is None or "ID" in state_allowlist:
        id_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_id_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("ID"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "ID")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                id_df = id_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(id_df))

    # Illinois
    if state_allowlist is None or "IL" in state_allowlist:
        il_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_il_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("IL"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "IL")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                il_df = il_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(il_df))

    # Indiana
    if state_allowlist is None or "IN" in state_allowlist:
        in_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_in_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("IN"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "IN")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                in_df = in_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(in_df))

    # Iowa
    if state_allowlist is None or "IA" in state_allowlist:
        ia_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ia_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("IA"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "IA")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ia_df = ia_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ia_df))

    # Kansas
    if state_allowlist is None or "KS" in state_allowlist:
        ks_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ks_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("KS"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "KS")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ks_df = ks_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ks_df))

    # Kentucky
    if state_allowlist is None or "KY" in state_allowlist:
        ky_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ky_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("KY"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "KY")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ky_df = ky_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ky_df))

    # Louisiana
    if state_allowlist is None or "LA" in state_allowlist:
        la_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_la_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("LA"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "LA")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                la_df = la_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(la_df))

    # Maine
    if state_allowlist is None or "ME" in state_allowlist:
        me_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_me_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("ME"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "ME")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                me_df = me_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(me_df))

    # Maryland
    if state_allowlist is None or "MD" in state_allowlist:
        md_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_md_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("MD"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "MD")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                md_df = md_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(md_df))

    # Massachusetts
    if state_allowlist is None or "MA" in state_allowlist:
        ma_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ma_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("MA"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "MA")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ma_df = ma_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ma_df))

    # Michigan
    if state_allowlist is None or "MI" in state_allowlist:
        mi_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_mi_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("MI"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "MI")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                mi_df = mi_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(mi_df))

    # Minnesota
    if state_allowlist is None or "MN" in state_allowlist:
        mn_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_mn_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("MN"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "MN")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                mn_df = mn_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(mn_df))

    # Mississippi
    if state_allowlist is None or "MS" in state_allowlist:
        ms_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ms_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("MS"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "MS")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ms_df = ms_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ms_df))

    # Missouri
    if state_allowlist is None or "MO" in state_allowlist:
        mo_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_mo_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("MO"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "MO")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                mo_df = mo_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(mo_df))

    # Montana
    if state_allowlist is None or "MT" in state_allowlist:
        mt_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_mt_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("MT"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "MT")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                mt_df = mt_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(mt_df))

    # Nebraska
    if state_allowlist is None or "NE" in state_allowlist:
        ne_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ne_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("NE"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "NE")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ne_df = ne_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ne_df))

    # Nevada
    if state_allowlist is None or "NV" in state_allowlist:
        nv_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_nv_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("NV"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "NV")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                nv_df = nv_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(nv_df))

    # New Hampshire
    if state_allowlist is None or "NH" in state_allowlist:
        nh_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_nh_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("NH"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "NH")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                nh_df = nh_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(nh_df))

    # New Jersey
    if state_allowlist is None or "NJ" in state_allowlist:
        nj_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_nj_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("NJ"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "NJ")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                nj_df = nj_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(nj_df))

    # New Mexico
    if state_allowlist is None or "NM" in state_allowlist:
        nm_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_nm_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("NM"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "NM")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                nm_df = nm_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(nm_df))

    # New York
    if state_allowlist is None or "NY" in state_allowlist:
        ny_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ny_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("NY"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "NY")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ny_df = ny_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ny_df))

    # North Carolina
    if state_allowlist is None or "NC" in state_allowlist:
        nc_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_nc_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("NC"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "NC")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                nc_df = nc_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(nc_df))

    # North Dakota
    if state_allowlist is None or "ND" in state_allowlist:
        nd_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_nd_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("ND"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "ND")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                nd_df = nd_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(nd_df))

    # Ohio
    if state_allowlist is None or "OH" in state_allowlist:
        oh_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_oh_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("OH"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "OH")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                oh_df = oh_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(oh_df))

    # Oklahoma
    if state_allowlist is None or "OK" in state_allowlist:
        ok_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ok_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("OK"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "OK")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ok_df = ok_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ok_df))

    # Oregon
    if state_allowlist is None or "OR" in state_allowlist:
        or_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_or_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("OR"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "OR")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                or_df = or_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(or_df))

    # Pennsylvania
    if state_allowlist is None or "PA" in state_allowlist:
        pa_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_pa_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("PA"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "PA")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                pa_df = pa_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(pa_df))

    # Rhode Island
    if state_allowlist is None or "RI" in state_allowlist:
        ri_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ri_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("RI"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "RI")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ri_df = ri_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ri_df))

    # South Carolina
    if state_allowlist is None or "SC" in state_allowlist:
        sc_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_sc_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("SC"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "SC")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                sc_df = sc_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(sc_df))

    # South Dakota
    if state_allowlist is None or "SD" in state_allowlist:
        sd_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_sd_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("SD"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "SD")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                sd_df = sd_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(sd_df))

    # Tennessee
    if state_allowlist is None or "TN" in state_allowlist:
        tn_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_tn_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("TN"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "TN")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                tn_df = tn_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(tn_df))

    # Texas
    if state_allowlist is None or "TX" in state_allowlist:
        tx_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_tx_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("TX"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "TX")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                tx_df = tx_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(tx_df))

    # Utah
    if state_allowlist is None or "UT" in state_allowlist:
        ut_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_ut_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("UT"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "UT")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                ut_df = ut_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(ut_df))

    # Vermont
    if state_allowlist is None or "VT" in state_allowlist:
        vt_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_vt_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("VT"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "VT")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                vt_df = vt_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(vt_df))

    # Virginia
    if state_allowlist is None or "VA" in state_allowlist:
        va_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_va_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("VA"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "VA")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                va_df = va_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(va_df))

    # Washington
    if state_allowlist is None or "WA" in state_allowlist:
        wa_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_wa_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("WA"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "WA")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                wa_df = wa_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(wa_df))

    # West Virginia
    if state_allowlist is None or "WV" in state_allowlist:
        wv_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_wv_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("WV"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "WV")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                wv_df = wv_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(wv_df))

    # Wisconsin
    if state_allowlist is None or "WI" in state_allowlist:
        wi_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_wi_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("WI"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "WI")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                wi_df = wi_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(wi_df))

    # Wyoming
    if state_allowlist is None or "WY" in state_allowlist:
        wy_df: DataFrame = dbt.ref(
            "stg_dbt_source__l2_s3_wy_haystaq_dna_flags"
        ).withColumn("state_postal_code", lit("WY"))
        if dbt.is_incremental and this_df is not None:
            max_loaded_at = (
                this_df.filter(col("state_postal_code") == "WY")
                .agg({"loaded_at": "max"})
                .collect()[0][0]
            )
            if max_loaded_at is not None:
                wy_df = wy_df.filter(col("loaded_at") > max_loaded_at)
        per_state_dfs.append(_cast_flag_columns(wy_df))

    if not per_state_dfs:
        raise ValueError(
            "No states selected for Haystaq flags. Set `l2_state_allowlist` (e.g. 'AK') to run a subset."
        )

    df = per_state_dfs[0]
    for next_df in per_state_dfs[1:]:
        df = df.unionByName(next_df, allowMissingColumns=True)

    if dbt.is_incremental:
        if df.count() == 0:
            return session.createDataFrame(data=[], schema=df.schema)

    return df
