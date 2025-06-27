import re
from typing import Any, Dict, List, Literal

import geopandas as gpd
import pandas as pd
from pyogrio.errors import DataSourceError
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    desc,
    first,
    lit,
    pandas_udf,
    row_number,
    struct,
    udf,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# See https://docs.google.com/spreadsheets/d/17iURGef8AKFokr5aJGciiF6lqYDKycGBP106-0Q7u5k/edit?gid=857937239#gid=857937239
# "election_type" tab, for a list of all possible `L2_district_type`s
# -----------------
# See this pinned message https://goodpartyorg.slack.com/archives/C08LC0W9L1W/p1743503690263989
# for a script on scraping all the shape files

# TIGER/Line layer codes used below
# ---------------------------------
# CD119 – 119th-Congress Congressional Districts
# SLDU  – State Legislative District, Upper chamber
# SLDL  – State Legislative District, Lower chamber
# ELSD  – Elementary School Districts
# SCSD  – Secondary (High) School Districts
# UNSD  – Unified School Districts
# SDADM – School-District Administrative Sub-areas (board zones, sub-districts)
# VTD   – Voting Districts (precincts, wards, city-council districts)
# PLACE – Incorporated Places / Census-Designated Places
# COUSUB – County Subdivisions (MCD/CCD)
# COUNTY – Counties & county-equivalents
# CONCITY – Consolidated Cities

L2_TO_TIGER_CODES: Dict[str, List[str]] = {
    # ------------------------------------------------------------------
    # LEGISLATIVE DISTRICTS
    # ------------------------------------------------------------------
    "US_Congressional_District": ["CD119"],
    "State_Senate_District": ["SLDU"],
    "State_House_District": ["SLDL"],
    "State_Board_of_Equalization": ["STATE"],
    "Land_Commission": ["STATE"],
    "Public_Regulation_Commission": ["STATE"],
    "Public_Service_Commission_District": ["STATE"],
    # ------------------------------------------------------------------
    # SCHOOL DISTRICTS (AND RELATED)
    # ------------------------------------------------------------------
    "Elementary_School_District": ["ELSD"],
    "High_School_District": ["SCSD"],
    "Secondary_School_District": ["SCSD"],
    "Unified_School_District": ["UNSD"],
    "School_District": ["ELSD", "SCSD", "UNSD"],
    # Sub-districts / board zones
    "School_Subdistrict": ["SDADM"],
    "Elementary_School_SubDistrict": ["SDADM"],
    "High_School_SubDistrict": ["SDADM"],
    "Unified_School_SubDistrict": ["SDADM"],
    "City_School_District": ["SDADM"],
    "School_District_Vocational": ["SDADM"],
    "School_Board_District": ["SDADM"],
    "Board_of_Education_District": ["SDADM"],
    "County_Board_of_Education_District": ["SDADM"],
    "County_Board_of_Education_SubDistrict": ["SDADM"],
    "College_Board_District": ["SDADM"],
    "Educational_Service_District": ["COUNTY"],
    "Educational_Service_Subdistrict": ["COUNTY"],
    "Superintendent_of_Schools_District": ["COUNTY"],
    "Community_College": ["COUNTY"],
    "Community_College_Commissioner_District": ["COUNTY"],
    "Community_College_SubDistrict": ["COUNTY"],
    "Learning_Community_Coordinating_Council_District": ["COUNTY"],
    # ------------------------------------------------------------------
    # MUNICIPAL / LOCAL POLITICAL DISTRICTS
    # ------------------------------------------------------------------
    "City_Council_Commissioner_District": ["PLACE"],
    "Proposed_City_Commissioner_District": ["PLACE"],
    "Municipal_Advisory_Council_District": ["PLACE"],
    "Municipal_Utility_District": ["PLACE"],
    "Municipal_Utility_SubDistrict": ["PLACE"],
    "Municipal_Water_District": ["PLACE"],
    "Municipal_Water_SubDistrict": ["PLACE"],
    "Police_District": ["PLACE"],
    "Community_Council_District": ["PLACE"],
    "Community_Council_SubDistrict": ["PLACE"],
    "Consolidated_City": ["CONCITY"],
    "City": ["COUNTY"],
    "County": ["COUNTY"],
    # ------------------------------------------------------------------
    # COUNTY-LEVEL DISTRICTS
    # ------------------------------------------------------------------
    "County_Commissioner_District": ["COUNTY"],
    "County_Supervisorial_District": ["COUNTY"],
    "County_Service_Area": ["COUNTY"],
    "County_Service_Area_SubDistrict": ["COUNTY"],
    "County_Water_District": ["COUNTY"],
    "County_Water_SubDistrict": ["COUNTY"],
    "County_Library_District": ["COUNTY"],
    "District_Attorney": ["COUNTY"],
    "Election_Commissioner_District": ["COUNTY"],
    "Assessment_District": ["COUNTY"],
    "Planning_Area_District": ["COUSUB"],  # possibly sub-county
    "Community_Planning_Area": ["COUSUB"],
    # ------------------------------------------------------------------
    # JUDICIAL DISTRICTS
    # ------------------------------------------------------------------
    "Judicial_Appellate_District": ["STATE"],
    "Judicial_Circuit_Court_District": ["COUNTY"],
    "Judicial_Sub_Circuit_District": ["COUNTY"],
    "Judicial_Superior_Court_District": ["COUNTY"],
    "Judicial_District": ["COUNTY"],
    "Judicial_District_Court_District": ["COUNTY"],
    "Judicial_Chancery_Court": ["COUNTY"],
    "Judicial_County_Board_of_Review_District": ["COUNTY"],
    "Judicial_County_Court_District": ["COUNTY"],
    "Judicial_Family_Court_District": ["COUNTY"],
    "Judicial_Juvenile_Court_District": ["COUNTY"],
    "Judicial_Magistrate_Division": ["COUNTY"],
    "Judicial_Supreme_Court_District": ["STATE"],
    # ------------------------------------------------------------------
    # INFRASTRUCTURE / UTILITY DISTRICTS
    # ------------------------------------------------------------------
    "Transit_District": ["COUNTY"],
    "Transit_SubDistrict": ["COUNTY"],
    "Metro_Transit_District": ["COUNTY"],
    "Rapid_Transit_District": ["COUNTY"],
    "Rapid_Transit_SubDistrict": ["PLACE"],
    "Water_District": ["COUNTY"],
    "Water_SubDistrict": ["COUNTY"],
    "Water_Agency": ["COUNTY"],
    "Water_Agency_SubDistrict": ["COUNTY"],
    "Water_Conservation_District": ["COUNTY"],
    "Water_Conservation_SubDistrict": ["COUNTY"],
    "Water_Replacement_SubDistrict": ["COUNTY"],
    "Power_District": ["COUNTY"],
    "Public_Utility_District": ["COUNTY"],
    "Public_Utility_SubDistrict": ["COUNTY"],
    "Wastewater_District": ["COUNTY"],
    "Sewer_District": ["COUNTY"],
    "Lighting_District": ["COUNTY"],
    "Garbage_District": ["COUNTY"],
    # ------------------------------------------------------------------
    # HEALTH / SAFETY DISTRICTS
    # ------------------------------------------------------------------
    "Fire_District": ["COUNTY"],
    "Fire_SubDistrict": ["COUNTY"],
    "Fire_Maintenance_District": ["COUNTY"],
    "Fire_Protection_District": ["COUNTY"],
    "Fire_Protection_SubDistrict": ["COUNTY"],
    "Mosquito_Abatement_District": ["COUNTY"],
    "Health_District": ["COUNTY"],
    "Paramedic_District": ["COUNTY"],
    "Hospital_SubDistrict": ["COUNTY"],
    "Law_Enforcement_District": ["COUNTY"],
    "Emergency_Communication_911_District": ["COUNTY"],
    "Emergency_Communication_911_SubDistrict": ["COUNTY"],
    "Vector_Control_District": ["COUNTY"],
    # ------------------------------------------------------------------
    # ENVIRONMENTAL / CONSERVATION
    # ------------------------------------------------------------------
    "Forest_Preserve": ["COUNTY"],
    "Conservation_District": ["COUNTY"],
    "Conservation_SubDistrict": ["COUNTY"],
    "Open_Space_District": ["COUNTY"],
    "Open_Space_SubDistrict": ["COUNTY"],
    "Resource_Conservation_District": ["COUNTY"],
    "Levee_District": ["COUNTY"],
    "Drainage_District": ["COUNTY"],
    "Flood_Control_Zone": ["COUNTY"],
    "Geological_Hazard_Abatement_District": ["COUNTY"],
    "Reclamation_District": ["COUNTY"],
    "River_Water_District": ["COUNTY"],
    # ------------------------------------------------------------------
    # SPECIAL DISTRICTS / OTHER LOCAL
    # ------------------------------------------------------------------
    "Community_Service_District": ["COUNTY"],
    "Community_Service_SubDistrict": ["COUNTY"],
    "Community_Facilities_District": ["COUNTY"],
    "Community_Facilities_SubDistrict": ["COUNTY"],
    "Park_District": ["COUNTY"],
    "Park_SubDistrict": ["COUNTY"],
    "Park_Commissioner_District": ["COUNTY"],
    "Cemetery_District": ["COUNTY"],
    "Memorial_District": ["COUNTY"],
    "TV_Translator_District": ["COUNTY"],
    "Airport_District": ["COUNTY"],
    "Improvement_Landowner_District": ["COUNTY"],
    "Facilities_Improvement_District": ["COUNTY"],
    "School_Facilities_Improvement_District": ["COUNTY"],
    "Rural_Service_District": ["COUNTY"],
    "Maintenance_District": ["COUNTY"],
    "Unincorporated_District": ["COUSUB"],
    "Multi_township_Assessor": ["COUSUB"],
    # ------------------------------------------------------------------
    # POLITICAL PARTY & ELECTION ADMIN
    # ------------------------------------------------------------------
    "Democratic_Convention_Member": ["STATE"],
    "Democratic_Zone": ["STATE"],
    "Republican_Convention_Member": ["STATE"],
    "Republican_Area": ["STATE"],
    "Central_Committee_District": ["COUNTY"],
    # ------------------------------------------------------------------
    # UNKNOWN OR CATCH-ALL TYPES
    # ------------------------------------------------------------------
    "Designated_Market_Area_DMA": ["COUNTY"],  # rolled up
    "Special_Tax_District": ["COUNTY"],
    "Other": ["STATE"],
    "Proposed_District": ["COUNTY"],
}

# ------------------------------------------------------------------
# Pattern-based roll-ups for everything *not* explicitly listed
# ------------------------------------------------------------------
FALLBACK_PATTERNS = [
    (re.compile(r"^County_"), ["COUNTY"]),
    (re.compile(r"_County_"), ["COUNTY"]),
    (re.compile(r"County$"), ["COUNTY"]),
    (re.compile(r"County_.*District"), ["COUNTY"]),
    #
    (re.compile(r"^City_"), ["PLACE"]),
    (re.compile(r"Municipal"), ["PLACE"]),
    (re.compile(r"Town(ship)?"), ["COUSUB"]),
    (re.compile(r"Village"), ["COUSUB"]),
    (re.compile(r"Unincorporated"), ["COUSUB"]),
]

THIS_TABLE_SCHEMA = StructType(
    [
        StructField("state", StringType(), True),
        StructField("office_type", StringType(), True),
        StructField("office_name", StringType(), True),
        StructField("ballots_projected", IntegerType(), True),
        StructField("inference_at", TimestampType(), True),
        StructField("election_year", IntegerType(), True),
        StructField("election_code", StringType(), True),
        StructField("model_version", StringType(), True),
        StructField("inferred_geoid", StringType(), True),
        StructField("fips_code", IntegerType(), True),
        StructField("place_name", StringType(), True),
        StructField("tiger_codes", ArrayType(StringType()), True),
        StructField("shapefile_paths", ArrayType(StringType()), True),
    ]
)


@pandas_udf(returnType=StringType())
def _add_geoid_to_voters(df: pd.DataFrame) -> pd.Series:
    """
    Add geoid to the dataframe. The input dataframe has the following columns:
    - Residence_Addresses_Longitude
    - Residence_Addresses_Latitude
    - office_type
    - office_name
    - shapefile_paths
    """
    most_common_geo_per_shapefile: List[Dict[Literal["geoid", "count"], Any]] = []

    for shapefile_path in df.iloc[0]["shapefile_paths"]:
        if shapefile_path == "MISSING_TIGER_CODE":
            continue

        # TODO: handle case where shapefile for that TIGER doesn't exist; use `COUNTY`
        # as a default backup.
        # load shapefile
        try:
            gdf_polygons = gpd.read_file(shapefile_path)
        except DataSourceError:
            continue

        # get point for each voter from their lat/long
        gdf_points = gpd.GeoDataFrame(
            data=df,
            geometry=gpd.points_from_xy(
                df["Residence_Addresses_Longitude"], df["Residence_Addresses_Latitude"]
            ),
            crs=gdf_polygons.crs,
        )

        # apply the spatial join
        joined = gpd.sjoin(gdf_points, gdf_polygons, how="inner", predicate="within")

        # compute the most prevalent geoid for each voter
        geoids_by_count = (
            joined.groupby(["office_type", "GEOID"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .drop_duplicates(subset=["office_type"])
            .sort_values("office_type")
        )

        if len(geoids_by_count) > 0:
            most_prevalent_geoid = geoids_by_count.iloc[0]["GEOID"]
            most_common_geo_per_shapefile.append(
                {
                    "geoid": most_prevalent_geoid,
                    "count": geoids_by_count.iloc[0]["count"],
                }
            )
        else:
            most_common_geo_per_shapefile.append(
                {"geoid": "NO_GEOID_FOUND", "count": 0}
            )

    # most prevalent geoid over all voters by count over all considered shapefiles
    try:
        most_prevalent_geoid = max(
            most_common_geo_per_shapefile, key=lambda x: x["count"]
        )["geoid"]
    except ValueError:
        most_prevalent_geoid = "NO_GEOID_FOUND"
    return pd.Series([str(most_prevalent_geoid) for x in df.index])


@udf(returnType=ArrayType(StringType()))
def _construct_shapefile_path(fips_code: int, tiger_codes: list[str]) -> list[str]:
    # Ensure FIPS code is two digits with leading zero if necessary
    formatted_fips = str(fips_code).zfill(2)
    lowered_tiger_codes = [tiger_code.lower() for tiger_code in tiger_codes]

    # Handle the 'county' and 'state' case for shapefile path construction
    # If the tiger_code is 'county', use 'us_county' in the path
    # If the tiger_code is 'state', use 'us_state' in the path
    # Otherwise, use the formatted fips and tiger_code as before
    shapefile_paths = []
    for tiger_code in lowered_tiger_codes:
        if tiger_code == "county":
            shapefile_paths.append(
                "/Volumes/goodparty_data_catalog/dbt/object_storage/shapefiles/us_census/tl_2024_us_county/"
            )
        elif tiger_code == "state":
            shapefile_paths.append(
                "/Volumes/goodparty_data_catalog/dbt/object_storage/shapefiles/us_census/tl_2024_us_state/"
            )
        elif tiger_code == "missing_tiger_code":
            shapefile_paths.append("MISSING_TIGER_CODE")
        else:
            shapefile_paths.append(
                f"/Volumes/goodparty_data_catalog/dbt/object_storage/shapefiles/us_census/tl_2024_{formatted_fips}_{tiger_code}/"
            )
    return shapefile_paths


@udf(returnType=ArrayType(StringType()))
def _get_tiger_codes(office_type: str) -> List[str]:
    """
    Maps an L2 office type to its corresponding TIGER/Line layer codes.

    Args:
        office_type: The L2 office type (e.g. "US_Congressional_District")

    Returns:
        List of TIGER/Line layer codes (e.g. ["CD119"])
    """
    try:
        tiger_code = L2_TO_TIGER_CODES[office_type]
    except KeyError:
        tiger_code = ["MISSING_TIGER_CODE"]
    return tiger_code


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=[
            "state",
            "office_type",
            "office_name",
            "election_year",
            "election_code",
            "model_version",
        ],
        on_schema_change="fail",
        tags=["voter_turnout", "geoid", "l2"],
    )

    # load fips codes
    fips_codes: DataFrame = dbt.ref("fips_codes")

    # load voter turnout
    # TODO: replace this with production-level predictions when available
    voter_turnout: DataFrame = dbt.ref(
        "stg_sandbox_source__turnout_projections_placeholder0"
    )

    # if incremental, filter to only inferences made after the last inference date in this table
    if dbt.is_incremental:
        this_table: DataFrame = session.table(f"{dbt.this}")
        max_inference_at_row = this_table.agg({"inference_at": "max"}).collect()[0]
        max_inference_at = max_inference_at_row[0] if max_inference_at_row else None

        if max_inference_at:
            voter_turnout = voter_turnout.filter(col("inference_at") > max_inference_at)

    # l2 uniform voter files
    l2_uniform_voter_files: DataFrame = dbt.ref("int__l2_nationwide_uniform")

    # for dev, downsample to certain states
    # TODO: remove this to run over all states
    # states_to_include = ["CA"]
    # states_to_include = ["ND", "VT", "WY", "DC", "AK", "SD", "MT", "RI", "DE", "HI"]
    # states_to_include = ["WA"]
    # voter_turnout = voter_turnout.filter(col("state").isin(states_to_include))

    # if voter_turnout has no rows, return an empty dataframe
    if voter_turnout.count() == 0:
        return session.createDataFrame(data=[], schema=THIS_TABLE_SCHEMA)

    # join over State to get the state fips code
    voter_turnout = (
        voter_turnout.alias("voter_turnout")
        .join(
            other=fips_codes.alias("fips_codes").select(
                col("fips_code"), col("place_name")
            ),
            on=col("voter_turnout.state") == col("fips_codes.place_name"),
            how="left",
        )
        .drop("fips_codes.place_name")
    )

    # determine relevant shapefiles according to state and office_type which requires mapping to TIGER code
    voter_turnout = voter_turnout.withColumn(
        "tiger_codes",
        _get_tiger_codes(col("office_type")),
    )
    voter_turnout = voter_turnout.withColumn(
        "shapefile_paths",
        _construct_shapefile_path(col("fips_code"), col("tiger_codes")),
    )

    # for each district prediction, look at each shapefile and pull the most prevalent geoid
    # for loop over each state since need to pass L2 voter data by state
    voter_turnout = voter_turnout.withColumn("geoid", lit(None))
    states = [
        row["state"] for row in voter_turnout.select(col("state")).distinct().collect()
    ]

    # TODO: remove downsampling
    # downsample states
    # states = states[:20]
    for state_num, state in enumerate(states):
        state_voter_turnout = voter_turnout.filter(col("state") == state)
        district_types_list = [
            row["office_type"]
            for row in state_voter_turnout.select("office_type").distinct().collect()
        ]

        for district_type_num, district_type in enumerate(district_types_list):
            # get voters by district type
            voters_by_district_type = l2_uniform_voter_files.select(
                [
                    district_type,
                    "Residence_Addresses_Longitude",
                    "Residence_Addresses_Latitude",
                    "state_postal_code",
                ]
            )

            # join voters by district type to voter turnout
            # TODO: replace this join with dynamic join on office_type and office_name
            # https://grok.com/share/bGVnYWN5_1f1642bc-219e-4db1-b7c2-fc00e78ac989
            voters_with_turnout: DataFrame = (
                voters_by_district_type.alias("voters")
                .join(
                    other=state_voter_turnout.alias("turnout"),
                    on=[
                        col("voters.state_postal_code") == col("turnout.state"),
                        col("voters." + district_type) == col("turnout.office_name"),
                    ],
                    how="inner",
                )
                .drop("voters.state_postal_code")
                .alias("voters_with_turnout")
            )

            # TODO: (update parameters as needed) downsample to 10% with some minimum and maximum value
            max_value_to_downsample = 1_000
            min_value_to_downsample = 1_000
            fraction_to_downsample = 0.1
            if (
                voters_with_turnout.count()
                > min_value_to_downsample / fraction_to_downsample
            ):
                voters_with_turnout = voters_with_turnout.sample(
                    fraction=fraction_to_downsample, seed=24601, withReplacement=False
                )
            voters_with_turnout = voters_with_turnout.limit(max_value_to_downsample)

            # Trigger a cache to ensure these transformations are applied before the filter
            voters_with_turnout.cache()

            # get the geoid for each voter by their (lat,long) for the given district
            voters_with_turnout = voters_with_turnout.withColumn(
                "inferred_geoid",
                _add_geoid_to_voters(
                    struct(
                        col("Residence_Addresses_Longitude"),
                        col("Residence_Addresses_Latitude"),
                        col("office_type"),
                        col("office_name"),
                        col("shapefile_paths"),
                    )
                ),
            )

            voters_with_turnout = voters_with_turnout.withColumn(
                "metainfo",
                lit(
                    f"state={state}, state_num={state_num}, district_type={district_type}, district_type_num={district_type_num}"
                ),
            )
            voters_with_turnout.cache()

            if state_num == 0 and district_type_num == 0:
                voter_turnout_w_geoid = voters_with_turnout
            else:
                voter_turnout_w_geoid = voter_turnout_w_geoid.union(voters_with_turnout)

        voter_turnout_w_geoid.cache()

    voter_turnout_w_geoid = voter_turnout_w_geoid.select(
        "office_type", "office_name", "state", "inferred_geoid", "metainfo"
    )

    # Group by district and select the most frequent inferred_geoid
    geoid_counts = voter_turnout_w_geoid.groupBy(
        "state", "office_type", "office_name", "inferred_geoid"
    ).agg(count("*").alias("geoid_count"), first("metainfo").alias("metainfo"))

    # Rank by frequency (most frequent first) and select the top one per district
    window_spec = Window.partitionBy("state", "office_type", "office_name").orderBy(
        desc("geoid_count"), desc("inferred_geoid")
    )

    voter_turnout_w_geoid = (
        geoid_counts.withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .drop("rank", "geoid_count")
    )

    # join inferred geoid back into voter turnout
    voter_turnout_w_geoid = voter_turnout_w_geoid.join(
        other=voter_turnout,
        on=["state", "office_type", "office_name"],
        how="left",
    )

    voter_turnout_w_geoid = voter_turnout_w_geoid.select(
        "state",
        "office_type",
        "office_name",
        "ballots_projected",
        "inference_at",
        "election_year",
        "election_code",
        "model_version",
        "inferred_geoid",
        "fips_code",
        "place_name",
        "tiger_codes",
        "shapefile_paths",
    ).distinct()

    # enforce table schema:
    voter_turnout_w_geoid_enforced = voter_turnout_w_geoid.select(
        *[
            col(field.name).cast(field.dataType).alias(field.name)
            for field in THIS_TABLE_SCHEMA
        ]
    )

    return voter_turnout_w_geoid_enforced
