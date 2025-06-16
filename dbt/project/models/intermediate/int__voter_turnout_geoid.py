import re
from typing import Dict, List

import geopandas as gpd
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import ArrayType, StringType

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


@pandas_udf(returnType=StringType())
def _add_geoid_to_voters(df: pd.DataFrame) -> pd.Series:
    """
    Add geoid to the dataframe. The input dataframe has the following columns:
    - Residence_Addresses_Longitude
    - Residence_Addresses_Latitude
    - state_postal_code
    - OfficeType
    - OfficeName
    - state
    """
    # all rows have the same shapefile path
    for shapefile_path in df.iloc[0]["shapefile_paths"]:
        # load shapefile
        gdf_polygons = gpd.read_file(shapefile_path)

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
        most_prevalent_geoids = (
            joined.groupby(["OfficeType", "GEOID"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .drop_duplicates(subset=["OfficeType"])
            .sort_values("OfficeType")
        )

    # There seems to be one clearly dominant geoid each run, so we return that
    return pd.Series([most_prevalent_geoids.iloc[0] for x in df.index])


@udf(returnType=ArrayType(StringType()))
def _construct_shapefile_path(fips_code: str, tiger_codes: list[str]) -> list[str]:
    return [
        f"/Volumes/goodparty_data_catalog/dbt/object_storage/census_shapefiles/tl_2024_{fips_code}_{tiger_code}/"
        for tiger_code in tiger_codes
    ]


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        incremental_strategy="merge",
        # unique_key=["officeName", "OfficeType", "state", "updated_at"],
        on_schema_change="fail",
        tags=["voter_turnout", "geoid", "l2"],
    )

    # load fips codes
    fips_codes: DataFrame = dbt.ref("fips_codes")

    # load voter turnout
    voter_turnout: DataFrame = dbt.ref(
        "stg_sandbox_source__turnout_projections_placeholder0"
    )

    # l2 uniform voter files
    l2_uniform_voter_files: DataFrame = dbt.ref("int__l2_nationwide_uniform")

    # for dev, restrict to CA:
    voter_turnout = voter_turnout.filter(col("state") == "CA")

    # further restrict to only the following offices:
    office_type_sublist = [
        "State_Senate_District",
        "Hospital_District",
        "Town_Ward",
        "County_Board_of_Education_District",
        "Fire_Protection_District",
    ]
    voter_turnout = voter_turnout.filter(col("officeType").isin(office_type_sublist))

    # determine relevant shapefiles according to state and office_type which requires mapping to TIGER code
    voter_turnout = voter_turnout.withColumn(
        "tiger_codes",
        udf(lambda x: L2_TO_TIGER_CODES[x], ArrayType(StringType()))(col("officeType")),
    )
    # join over State to get the state fips code
    voter_turnout = voter_turnout.join(
        other=fips_codes.select(col("fips_code"), col("place_name")),
        on=col("state") == col("place_name"),
        how="left",
    )

    # construct shapefile path:
    # /Volumes/goodparty_data_catalog/dbt/object_storage/census_shapefiles/tl_2024_{fips_code}_{tiger_code}/
    voter_turnout = voter_turnout.withColumn(
        "tiger_codes",
        udf(lambda x: L2_TO_TIGER_CODES[x], ArrayType(StringType()))(col("officeType")),
    )
    voter_turnout = voter_turnout.withColumn(
        "shapefile_paths",
        _construct_shapefile_path(col("fips_code"), col("tiger_codes")),
    )

    # for each district prediction, look at each shapefile and pull the most prevalent geoid
    # for loop over each state since need to pass L2 voter data by state
    states = [
        row["state"] for row in voter_turnout.select(col("state")).distinct().collect()
    ]
    for state in states:
        state_voter_turnout = voter_turnout.filter(col("state") == state)
        district_types_list = [
            row["officeType"]
            for row in state_voter_turnout.select("officeType").distinct().collect()
        ]

        for district_type in district_types_list:
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
            voters_with_turnout: DataFrame = voters_by_district_type.join(
                other=state_voter_turnout,
                on=[
                    voters_by_district_type.state_postal_code
                    == state_voter_turnout.state,
                    voters_by_district_type[district_type]
                    == state_voter_turnout.officeName,
                ],
                how="inner",
            )

            # get the geoid for each voter by their (lat,long) for the given district
            voters_with_turnout = voters_with_turnout.withColumn(
                "geoid",
                _add_geoid_to_voters(
                    col("shapefile_paths"),
                    col("state"),
                    col("officeType"),
                    col("officeName"),
                ),
            )

            voters_with_turnout = voters_with_turnout.select(
                "officeType", "officeName", "state", "geoid"
            ).distinct()

            # add geoid back into the voter turnout dataframe
            voter_turnout = voter_turnout.join(
                other=voters_with_turnout.select(
                    "officeType", "officeName", "state", "geoid"
                ),
                on=[
                    voter_turnout.state == voters_with_turnout.state,
                    voter_turnout.officeName == voters_with_turnout.officeName,
                    voter_turnout.officeType == voters_with_turnout.officeType,
                ],
                how="left",
            )

    return voters_with_turnout
