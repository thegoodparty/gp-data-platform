import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# See https://docs.google.com/spreadsheets/d/17iURGef8AKFokr5aJGciiF6lqYDKycGBP106-0Q7u5k/edit?gid=857937239#gid=857937239
# "election_type" tab, for a list of all possible `L2_district_type`s
# -----------------
# See this pinned message https://goodpartyorg.slack.com/archives/C08LC0W9L1W/p1743503690263989
# for a script on scraping all the shape files
# Hopefully they can be combined into one file so we don't need mapping logic between L2_district_type + state -> shapefile
def enrich_with_geoids(shapefile_path, demographic_path, L2_district_type, L2_district_name):
    print("L2_district_type: ", L2_district_type)
    print("L2_district_name: ", L2_district_name)

    demographic_df = pd.read_csv(demographic_path, low_memory=False)

    series_clean = (demographic_df[L2_district_type]
                    .astype(str)
                    .str.lstrip("0") # Nigel's turnout table has leading zeros, demographic files do not
                    .str.strip())

    target = str(L2_district_name).lstrip("0").strip()

    filtered_demographic = demographic_df[series_clean == target][[
        'Residence_Addresses_Longitude',
        'Residence_Addresses_Latitude',
        L2_district_type
    ]]
    excluded_rows = demographic_df[demographic_df[L2_district_type] != L2_district_name]
    print("excluded demographic rows: ", excluded_rows[L2_district_type].head())
    print("filtered demographic length:", len(filtered_demographic))
    print("filtered demographic head: ", filtered_demographic.head())

    gdf_polygons = gpd.read_file(shapefile_path)
    print("gdf_polygons length: ", len(gdf_polygons))
    
    print(gdf_polygons.head())
    
    gdf_points = gpd.GeoDataFrame(
        filtered_demographic,
        geometry=gpd.points_from_xy(filtered_demographic["Residence_Addresses_Longitude"], filtered_demographic["Residence_Addresses_Latitude"]),
        crs=gdf_polygons.crs
    )
    print("gdf points length: ", len(gdf_points))

    joined = gpd.sjoin(gdf_points, gdf_polygons, how="inner", predicate="within")
    print("joined length: ", len(joined))

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
    # Or more likely add them all to an array so during matching we can go down the list until we have a match in the BR data?
    print("Printing frequency_table")
    print(frequency_table)

    print("Printing most_prevalent_geoids")
    print(most_prevalent_geoids)


def main():
    turnout_df = pd.read_csv('resources/turnout_projections_sample.csv')
    first_row_df = turnout_df.iloc[0]
    
    L2_district_type = first_row_df['OfficeType']
    L2_district_name = first_row_df['OfficeName']

    enrich_with_geoids(
                        "./shapefiles/tl_2024_06_sldl_ca.shp", 
                       "./resources/ca_demographic_sample.csv", 
                       L2_district_type, 
                       L2_district_name
                       )

main()






