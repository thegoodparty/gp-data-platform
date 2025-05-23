import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("WinNumber") \
    .getOrCreate()

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

    most_prevalent_geoids = (
        joined.groupby([L2_district_type, "GEOID"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .drop_duplicates(subset=[L2_district_type])
        .sort_values(L2_district_type)
    )

    print(most_prevalent_geoids)



