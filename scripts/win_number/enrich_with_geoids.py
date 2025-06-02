import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import re

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
    # ------------------------------------------------------------------
    # LEGISLATIVE DISTRICTS
    # ------------------------------------------------------------------
    "US_Congressional_District": ["CD118"],
    "State_Senate_District":     ["SLDU"],
    "State_House_District":      ["SLDL"],
    "State_Board_of_Equalization": ["STATE"],
    "Land_Commission":           ["STATE"],
    "Public_Regulation_Commission": ["STATE"],
    "Public_Service_Commission_District": ["STATE"],

    # ------------------------------------------------------------------
    # SCHOOL DISTRICTS (AND RELATED)
    # ------------------------------------------------------------------
    "Elementary_School_District":           ["ELSD"],
    "High_School_District":                 ["SCSD"],
    "Secondary_School_District":            ["SCSD"],
    "Unified_School_District":              ["UNSD"],
    "School_District":                      ["ELSD", "SCSD", "UNSD"],

    # Sub-districts / board zones
    "School_Subdistrict":                   ["SDADM"],
    "Elementary_School_SubDistrict":        ["SDADM"],
    "High_School_SubDistrict":              ["SDADM"],
    "Unified_School_SubDistrict":           ["SDADM"],
    "City_School_District":                 ["SDADM"],
    "School_District_Vocational":           ["SDADM"],
    "School_Board_District":                ["SDADM"],
    "Board_of_Education_District":          ["SDADM"],
    "County_Board_of_Education_District":   ["SDADM"],
    "County_Board_of_Education_SubDistrict":["SDADM"],
    "College_Board_District":               ["SDADM"],
    "Educational_Service_District":         ["COUNTY"],
    "Educational_Service_Subdistrict":      ["COUNTY"],
    "Superintendent_of_Schools_District":   ["COUNTY"],
    "Community_College":                    ["COUNTY"],
    "Community_College_Commissioner_District": ["COUNTY"],
    "Community_College_SubDistrict":        ["COUNTY"],
    "Learning_Community_Coordinating_Council_District": ["COUNTY"],

    # ------------------------------------------------------------------
    # MUNICIPAL / LOCAL POLITICAL DISTRICTS
    # ------------------------------------------------------------------
    "City_Council_Commissioner_District": ["PLACE"],
    "Proposed_City_Commissioner_District": ["PLACE"],
    "Municipal_Advisory_Council_District": ["PLACE"],
    "Municipal_Utility_District":          ["PLACE"],
    "Municipal_Utility_SubDistrict":       ["PLACE"],
    "Municipal_Water_District":            ["PLACE"],
    "Municipal_Water_SubDistrict":         ["PLACE"],
    "Police_District":                     ["PLACE"],
    "Community_Council_District":          ["PLACE"],
    "Community_Council_SubDistrict":       ["PLACE"],
    "Consolidated_City":                   ["CONCITY"],

    # ------------------------------------------------------------------
    # COUNTY-LEVEL DISTRICTS
    # ------------------------------------------------------------------
    "County_Commissioner_District":        ["COUNTY"],
    "County_Supervisorial_District":       ["COUNTY"],
    "County_Service_Area":                 ["COUNTY"],
    "County_Service_Area_SubDistrict":     ["COUNTY"],
    "County_Water_District":               ["COUNTY"],
    "County_Water_SubDistrict":            ["COUNTY"],
    "County_Library_District":             ["COUNTY"],
    "District_Attorney":                   ["COUNTY"],
    "Election_Commissioner_District":      ["COUNTY"],
    "Assessment_District":                 ["COUNTY"],
    "Planning_Area_District":              ["COUSUB"],  # possibly sub-county
    "Community_Planning_Area":             ["COUSUB"],

    # ------------------------------------------------------------------
    # JUDICIAL DISTRICTS
    # ------------------------------------------------------------------
    "Judicial_Appellate_District":         ["STATE"],
    "Judicial_Circuit_Court_District":     ["COUNTY"],
    "Judicial_Sub_Circuit_District":       ["COUNTY"],
    "Judicial_Superior_Court_District":    ["COUNTY"],
    "Judicial_District":                   ["COUNTY"],
    "Judicial_District_Court_District":    ["COUNTY"],
    "Judicial_Chancery_Court":             ["COUNTY"],
    "Judicial_County_Board_of_Review_District": ["COUNTY"],
    "Judicial_County_Court_District":      ["COUNTY"],
    "Judicial_Family_Court_District":      ["COUNTY"],
    "Judicial_Juvenile_Court_District":    ["COUNTY"],
    "Judicial_Magistrate_Division":        ["COUNTY"],
    "Judicial_Supreme_Court_District":     ["STATE"],

    # ------------------------------------------------------------------
    # INFRASTRUCTURE / UTILITY DISTRICTS
    # ------------------------------------------------------------------
    "Transit_District":                    ["COUNTY"],
    "Transit_SubDistrict":                ["COUNTY"],
    "Metro_Transit_District":             ["COUNTY"],
    "Rapid_Transit_District":             ["COUNTY"],
    "Rapid_Transit_SubDistrict":          ["PLACE"],
    "Water_District":                     ["COUNTY"],
    "Water_SubDistrict":                  ["COUNTY"],
    "Water_Agency":                       ["COUNTY"],
    "Water_Agency_SubDistrict":           ["COUNTY"],
    "Water_Conservation_District":        ["COUNTY"],
    "Water_Conservation_SubDistrict":     ["COUNTY"],
    "Water_Replacement_SubDistrict":      ["COUNTY"],
    "Power_District":                     ["COUNTY"],
    "Public_Utility_District":            ["COUNTY"],
    "Public_Utility_SubDistrict":         ["COUNTY"],
    "Wastewater_District":                ["COUNTY"],
    "Sewer_District":                     ["COUNTY"],
    "Lighting_District":                  ["COUNTY"],
    "Garbage_District":                   ["COUNTY"],

    # ------------------------------------------------------------------
    # HEALTH / SAFETY DISTRICTS
    # ------------------------------------------------------------------
    "Fire_District":                      ["COUNTY"],
    "Fire_SubDistrict":                  ["COUNTY"],
    "Fire_Maintenance_District":         ["COUNTY"],
    "Fire_Protection_District":          ["COUNTY"],
    "Fire_Protection_SubDistrict":       ["COUNTY"],
    "Mosquito_Abatement_District":       ["COUNTY"],
    "Health_District":                   ["COUNTY"],
    "Paramedic_District":                ["COUNTY"],
    "Hospital_SubDistrict":              ["COUNTY"],
    "Law_Enforcement_District":          ["COUNTY"],
    "Emergency_Communication_911_District": ["COUNTY"],
    "Emergency_Communication_911_SubDistrict": ["COUNTY"],
    "Vector_Control_District":           ["COUNTY"],

    # ------------------------------------------------------------------
    # ENVIRONMENTAL / CONSERVATION
    # ------------------------------------------------------------------
    "Forest_Preserve":                   ["COUNTY"],
    "Conservation_District":            ["COUNTY"],
    "Conservation_SubDistrict":         ["COUNTY"],
    "Open_Space_District":              ["COUNTY"],
    "Open_Space_SubDistrict":           ["COUNTY"],
    "Resource_Conservation_District":   ["COUNTY"],
    "Levee_District":                   ["COUNTY"],
    "Drainage_District":                ["COUNTY"],
    "Flood_Control_Zone":               ["COUNTY"],
    "Geological_Hazard_Abatement_District": ["COUNTY"],
    "Reclamation_District":             ["COUNTY"],
    "River_Water_District":             ["COUNTY"],

    # ------------------------------------------------------------------
    # SPECIAL DISTRICTS / OTHER LOCAL
    # ------------------------------------------------------------------
    "Community_Service_District":       ["COUNTY"],
    "Community_Service_SubDistrict":    ["COUNTY"],
    "Community_Facilities_District":    ["COUNTY"],
    "Community_Facilities_SubDistrict": ["COUNTY"],
    "Park_District":                    ["COUNTY"],
    "Park_SubDistrict":                 ["COUNTY"],
    "Park_Commissioner_District":       ["COUNTY"],
    "Cemetery_District":                ["COUNTY"],
    "Memorial_District":                ["COUNTY"],
    "TV_Translator_District":           ["COUNTY"],
    "Airport_District":                 ["COUNTY"],
    "Improvement_Landowner_District":   ["COUNTY"],
    "Facilities_Improvement_District":  ["COUNTY"],
    "School_Facilities_Improvement_District": ["COUNTY"],
    "Rural_Service_District":           ["COUNTY"],
    "Maintenance_District":             ["COUNTY"],
    "Unincorporated_District":          ["COUSUB"],
    "Multi_township_Assessor":          ["COUSUB"],

    # ------------------------------------------------------------------
    # POLITICAL PARTY & ELECTION ADMIN
    # ------------------------------------------------------------------
    "Democratic_Convention_Member":     ["STATE"],
    "Democratic_Zone":                  ["STATE"],
    "Republican_Convention_Member":     ["STATE"],
    "Republican_Area":                  ["STATE"],
    "Central_Committee_District":       ["COUNTY"],

    # ------------------------------------------------------------------
    # UNKNOWN OR CATCH-ALL TYPES
    # ------------------------------------------------------------------
    "Designated_Market_Area_DMA":       ["COUNTY"],  # rolled up
    "Special_Tax_District":             ["COUNTY"],
    "Other":                            ["STATE"],
    "Proposed_District":                ["COUNTY"],
}

# ------------------------------------------------------------------
# Pattern-based roll-ups for everything *not* explicitly listed
# ------------------------------------------------------------------
FALLBACK_PATTERNS = [
    (re.compile(r"^County_"),           ["COUNTY"]),
    (re.compile(r"_County_"),           ["COUNTY"]),
    (re.compile(r"County$"),            ["COUNTY"]),
    (re.compile(r"County_.*District"),  ["COUNTY"]),
    #
    (re.compile(r"^City_"),             ["PLACE"]),
    (re.compile(r"Municipal"),          ["PLACE"]),
    (re.compile(r"Town(ship)?"),        ["COUSUB"]),
    (re.compile(r"Village"),            ["COUSUB"]),
    (re.compile(r"Unincorporated"),     ["COUSUB"]),
]

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

    
    print("Printing frequency_table")
    print(frequency_table)

    print("Printing most_prevalent_geoids")
    print(most_prevalent_geoids)

    # There seems to be one clearly dominant geoid each run, so we return that
    return most_prevalent_geoids.iloc[0]

def test_ca_state_house_18(turnout_df):
    
    first_row_df = turnout_df.iloc[0]
    
    L2_district_type = first_row_df['OfficeType']
    L2_district_name = first_row_df['OfficeName']

    shapefile_paths = determine_shapefile_path(L2_district_name, 'ca')

    geoids = []

    for path in shapefile_paths:
        geoid = enrich_with_geoids(
                        path, 
                       "./resources/ca_demographic_sample_house_018.csv", 
                       L2_district_type, 
                       L2_district_name
                       )
        if (geoid):
            geoids.push(geoid)
    
    return geoids
    
def test_ca_dinuba_city_council(turnout_df):
    dinuba_city_row = turnout_df.iloc[2]

    L2_district_type = dinuba_city_row['OfficeType']
    L2_district_name = dinuba_city_row['OfficeName']

    geoids = []

    shapefile_paths = determine_shapefile_path(L2_district_name, 'ca')

    for path in shapefile_paths:
        geoid = enrich_with_geoids(
                    path, 
                    "./resources/ca_demographic_sample_dinuba.csv", 
                    L2_district_type, 
                    L2_district_name
                    )
        if (geoid):
            geoids.push(geoid)
    
    return geoids

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
    # house_18_geoids = test_ca_state_house_18(turnout_df)
    dinuba_geoids = test_ca_dinuba_city_council(turnout_df)

    # Add the geoids to a table in election-api alongside their corresponding turnout counts based on matching
    # L2_district_type and L2_district_name


    

    


main()






