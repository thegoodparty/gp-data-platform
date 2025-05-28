import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("WinNumber") \
    .getOrCreate()

# See https://docs.google.com/spreadsheets/d/17iURGef8AKFokr5aJGciiF6lqYDKycGBP106-0Q7u5k/edit?gid=857937239#gid=857937239
# "election_type" tab, for a list of all possible `L2_district_type`s
# -----------------
# See this pinned message https://goodpartyorg.slack.com/archives/C08LC0W9L1W/p1743503690263989
# for a script on scraping all the shape files
# Hopefully they can be combined into one file so we don't need mapping logic between L2_district_type + state -> shapefile
def enrich_with_geoids(shapefile_path, L2_district_type):
    query = f"""
SELECT Residence_Addresses_Longitude, Residence_Addresses_Latitude, {L2_district_type}
FROM goodparty_data_catalog.dbt_hugh_source.l2_s3_wy_demographic
"""


    df_spark = spark.sql(query)
    df_pandas = df_spark.toPandas()

    gdf_polygons = gpd.read_file(shapefile_path)
    
    print(gdf_polygons.head())
    
    gdf_points = gpd.GeoDataFrame(
        df_pandas,
        geometry=gpd.points_from_xy(df_pandas["Residence_Addresses_Longitude"], df_pandas["Residence_Addresses_Latitute"]),
        crs=gdf_polygons.crs
    )

    joined = gpd.sjoin(gdf_points, gdf_polygons, how="inner", predicate="within")

    frequency_table = joined.groupby(["GEOID", L2_district_type]).size().unstack(fill_value=0)
    # Need logic to determine which layer of geoids / MTFCC we need. Many layers of districts on any one given lat long
    most_prevalent_geoids = (
        joined.groupby([L2_district_type, "GEOID"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .drop_duplicates(subset=[L2_district_type])
        .sort_values(L2_district_type)
    )

    # Unknown: should we just always use the top most frequently occurring geoid? 
    # Or more likely go down the list until we have a match in the BR data?

    print(most_prevalent_geoids)



enrich_with_geoids("../shapefiles/tl_2024_48_unsd.shp", )



