import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# See https://docs.google.com/spreadsheets/d/17iURGef8AKFokr5aJGciiF6lqYDKycGBP106-0Q7u5k/edit?gid=857937239#gid=857937239
# "election_type" tab, for a list of all possible `L2_district_type`s
# -----------------
# See this pinned message https://goodpartyorg.slack.com/archives/C08LC0W9L1W/p1743503690263989
# for a script on scraping all the shape files

# TIGER/Line layer codes used below
# ---------------------------------
# CD118 – 118th-Congress Congressional Districts
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

L2_TO_TIGER_CODES: dict[str, list[str]] = {
    # ------------- Legislative -------------
    "US_Congressional_District": ["CD118"],
    "State_Senate_District":      ["SLDU"],
    "State_House_District":       ["SLDL"],

    # ------------- School districts -------------
    "Elementary_School_District":           ["ELSD"],
    "High_School_District":                 ["SCSD"],
    "Secondary_School_District":            ["SCSD"],
    "Unified_School_District":              ["UNSD"],
    "School_District":                      ["ELSD", "SCSD", "UNSD"],
    # sub-districts, boards, vocational, etc.
    "City_School_District":                 ["SDADM"],
    "Board_of_Education_District":          ["SDADM"],
    "School_District_Vocational":           ["SDADM"],
    "School_Board_District":                ["SDADM"],
    "School_Subdistrict":                   ["SDADM"],
    "Unified_School_SubDistrict":           ["SDADM"],
    "Elementary_School_SubDistrict":        ["SDADM"],
    "High_School_SubDistrict":              ["SDADM"],
    "County_Board_of_Education_District":   ["SDADM"],
    "County_Board_of_Education_SubDistrict":["SDADM"],

    # ------------- Voting districts / wards -------------
    "City_Council_Commissioner_District": ["VTD", "PLACE"], # Roll up to PLACE, or maybe should always roll up to PLACE

    # ------------- Municipal & county boundaries -------------
    # Incorporated places / CDPs
    "Proposed_City_Commissioner_District": ["PLACE"],
    # Unincorporated or township-style areas
    "Unincorporated_District": ["COUSUB"],
    "Community_Planning_Area":  ["COUSUB"],
    "Planning_Area_District":   ["COUSUB"],
    # County-wide service areas
    "County_Service_Area":            ["COUNTY"],
    "County_Service_Area_SubDistrict":["COUNTY"],

    # ------------- Consolidated cities -------------
    "Consolidated_City": ["CONCITY"],

    # ------------- Everything the Census DOESN’T publish -------------
    # "County_Commissioner_District":      [],
    # "County_Supervisorial_District":     [],
    # "Designated_Market_Area_DMA":        [],
    # "Water_District":                    [],
    # "Fire_District":                     [],
}

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

    return most_prevalent_geoids.iloc[0]

def test_ca_state_house_18(turnout_df, shapefile_path):
    
    first_row_df = turnout_df.iloc[0]
    
    L2_district_type = first_row_df['OfficeType']
    L2_district_name = first_row_df['OfficeName']

    shapefile_paths = determine_shapefile_path(L2_district_name, 'ca')

    for path in shapefile_paths:
        geoid = enrich_with_geoids(
                        path, 
                       "./resources/ca_demographic_sample_house_018.csv", 
                       L2_district_type, 
                       L2_district_name
                       )
        if (geoid):
            return geoid
    
def test_ca_dinuba_city_council(turnout_df, shapefile_path):
    dinuba_city_row = turnout_df.iloc[2]

    L2_district_type = dinuba_city_row['OfficeType']
    L2_district_name = dinuba_city_row['OfficeName']

    return enrich_with_geoids(
                    "./shapefiles/ca_places/tl_2024_06_place.shp", 
                    "./resources/ca_demographic_sample_dinuba.csv", 
                    L2_district_type, 
                    L2_district_name
                    )

def determine_shapefile_path(L2_district_type, state):
    # state being a two letter abbreviation

    # shapefiles should follow a convention: year_type_state
    base_path = "./shapefiles/tl_2024_06"
    types = L2_TO_TIGER_CODES[L2_district_type]

    paths = []

    for type in types:
        shapefile_path = base_path + type.lower() + state.lower()
        paths.append(shapefile_path)
    

def main():
    turnout_df = pd.read_csv('resources/turnout_projections_sample.csv')
    # geoid = test_ca_state_house_18(turnout_df)
    geoid = test_ca_dinuba_city_council(turnout_df)


    

    


main()






