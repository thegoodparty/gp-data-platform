{# Column documentation for serve_agent_voters.
   Auto-generated from seeds/serve_agent_voters_columns.csv, whose
   descriptions are sourced from L2's own data dictionaries
   (per-state uniform data dictionary; sandbox.haystaq_data_dictionary). #}

{% docs sav_voter_key %}
Stable pseudonymous voter key: SHA-256 hash of L2's LALVOTERID. Uniquely identifies a constituent for counting/joining without exposing the underlying L2 identifier. One row per voter.
{% enddocs %}

{% docs sav_Residence_Addresses_City %}
City of the voter's residence address.
{% enddocs %}

{% docs sav_Residence_Addresses_State %}
Two-letter US state of the voter's residence address.
{% enddocs %}

{% docs sav_Residence_Addresses_Zip %}
5-digit ZIP code of the voter's residence address.
{% enddocs %}

{% docs sav_Residence_Addresses_CensusTract %}
2020 Census tract of the voter's residence (geographic aggregation unit, not a street address).
{% enddocs %}

{% docs sav_Residence_Addresses_CensusBlockGroup %}
2020 Census block group of the voter's residence (geographic aggregation unit, not a street address).
{% enddocs %}

{% docs sav_Voters_Gender %}
'M', 'F' or blank.
{% enddocs %}

{% docs sav_Voters_Age %}
Age is based on current year minus year of birth.
{% enddocs %}

{% docs sav_ConsumerData_Education_of_Person %}
Modeled likely level of education of person.
{% enddocs %}

{% docs sav_CountyEthnic_Description %}
As reported by election authority.
{% enddocs %}

{% docs sav_CountyEthnic_LALEthnicCode %}
See File Layout document, Ethnic tab
{% enddocs %}

{% docs sav_Ethnic_Description %}
Ethnic Description
{% enddocs %}

{% docs sav_EthnicGroups_EthnicGroup1Desc %}
See File Layout document, Ethnic tab
{% enddocs %}

{% docs sav_ConsumerData_Hispanic_Country_Code %}
Likely Hispanic country of origin.
{% enddocs %}

{% docs sav_ConsumerData_Inferred_HH_Rank %}
Indicates the Inferred household rank of the individual name appearing on customer's file. Rank is a rank that is assigned to each individual in the household based on age.
{% enddocs %}

{% docs sav_ConsumerData_Language_Code %}
Modeled using both first and last name applying ethnolinguistic and geocentric rules to both the surname prefix and suffix and identifies the specific ethnic, religious, and minority status of individuals
{% enddocs %}

{% docs sav_ConsumerData_Marital_Status %}
Inferred marital status of individual.
{% enddocs %}

{% docs sav_ConsumerData_Single_Parent_in_Household %}
Modeled and self-reported data that a single parent lives at this household.
{% enddocs %}

{% docs sav_ConsumerDataLL_Veteran %}
Yes or null
{% enddocs %}

{% docs sav_Voters_PlaceOfBirth %}
Unformatted and non-standardized text field available only in some states.
{% enddocs %}

{% docs sav_Voters_Active %}
A = Active, I = Inactive
{% enddocs %}

{% docs sav_Voters_CalculatedRegDate %}
format '99/99/9999'; calculated reg date will be the same as official reg date except in those cases where the county has used it as a record update field in which case the calculated registration date will be set to 30 days prior to the oldest recorded vote date as a better indicator of the true registration date of the voter
{% enddocs %}

{% docs sav_Voters_OfficialRegDate %}
As reported by election authority.
{% enddocs %}

{% docs sav_AbsenteeTypes_Description %}
Military, Long Term, In State, Out of State, Permanent Out of Country, Mail Precinct, Special, Temporary, Permanent U.S., Federal
{% enddocs %}

{% docs sav_ConsumerData_Females_in_HH_18_24 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Females_in_HH_25_34 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Females_in_HH_35_44 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Females_in_HH_45_54 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Females_in_HH_55_64 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Females_in_HH_65_74 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Females_in_HH_75_Plus %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Males_in_HH_18_24 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Males_in_HH_25_34 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Males_in_HH_35_44 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Males_in_HH_45_54 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Males_in_HH_55_64 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Males_in_HH_65_74 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Males_in_HH_75_Plus %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Unknown_Gender_in_HH_18_24 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Unknown_Gender_in_HH_25_34 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Unknown_Gender_in_HH_35_44 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Unknown_Gender_in_HH_45_54 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Unknown_Gender_in_HH_55_64 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Unknown_Gender_in_HH_65_74 %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Unknown_Gender_in_HH_75_Plus %}
Presence of household members by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_00_02 %}
Presence of children in household by age.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_00_02_Female %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_00_02_Male %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_00_02_Unknown %}
Presence of children in household by age and gender where gender is unknown.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_11_15 %}
Presence of children in household by age.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_11_15_Female %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_11_15_Male %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_11_15_Unknown %}
Presence of children in household by age and gender where gender is unknown.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_16_17 %}
Presence of children in household by age.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_16_17_Female %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_16_17_Male %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_16_17_Unknown %}
Presence of children in household by age and gender where gender is unknown.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_03_05 %}
Presence of children in household by age.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_03_05_Female %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_03_05_Male %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_03_05_Unknown %}
Presence of children in household by age and gender where gender is unknown.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_06_10 %}
Presence of children in household by age.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_06_10_Female %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_06_10_Male %}
Presence of children in household by age and gender.
{% enddocs %}

{% docs sav_ConsumerData_Children_in_HH_Age_06_10_Unknown %}
Presence of children in household by age and gender where gender is unknown.
{% enddocs %}

{% docs sav_ConsumerData_Number_Of_Adults_in_HH %}
Indicates the number of adults in the household. An adult is anyone 18 years old or older living in a household. Not limited to voters.
{% enddocs %}

{% docs sav_ConsumerData_Number_Of_Children_in_HH %}
Number of children found in household.
{% enddocs %}

{% docs sav_ConsumerData_Number_Of_Persons_in_HH %}
Indicates the total number of occupants in the household. Household size is calculated by adding Number of Adults and Number of Children and is not limited to voters.
{% enddocs %}

{% docs sav_ConsumerData_Disabled_In_HH %}
Someone in the household is disabled.
{% enddocs %}

{% docs sav_ConsumerData_Generations_In_HH %}
Number of generations found within household.
{% enddocs %}

{% docs sav_ConsumerData_Presence_Of_Children_in_HH %}
Indicates the known presence/absence of children age 0-17 in the household. The Number of Children feeds this element along with other contributor's data.
{% enddocs %}

{% docs sav_ConsumerData_Senior_Adult_In_HH %}
Indicates that there is a senior adult in the household where another adult is identified as the 1st individual. (Senior adult is age 55+.).
{% enddocs %}

{% docs sav_ConsumerData_Veteran_In_HH %}
Someone within the household is a U.S. military veteran.
{% enddocs %}

{% docs sav_ConsumerData_Young_Adult_In_HH %}
Indicates that there is a young adult in the household where another adult is identified as the 1st individual. (Young adult is ages 18 to 25.)
{% enddocs %}

{% docs sav_Residence_HHGender_Description %}
Cannot Determine,Female Only Household,Male Only Household,Mixed Gender Household, based on all individuals in the HH not limited to just voters
{% enddocs %}

{% docs sav_Residence_Families_HHVotersCount %}
Residence Families HHVotersCount
{% enddocs %}

{% docs sav_County %}
County of the voter's residence.
{% enddocs %}

{% docs sav_Voters_FIPS %}
County FIPS code of the voter's residence.
{% enddocs %}

{% docs sav_County_Commissioner_District %}
County Commissioner District
{% enddocs %}

{% docs sav_County_Supervisorial_District %}
County Supervisorial District
{% enddocs %}

{% docs sav_Precinct %}
Voting precinct of the voter's residence.
{% enddocs %}

{% docs sav_US_Congressional_District %}
US Congressional District
{% enddocs %}

{% docs sav_State_Senate_District %}
State Senate District
{% enddocs %}

{% docs sav_State_House_District %}
State House District
{% enddocs %}

{% docs sav_State_Legislative_District %}
State Legislative District
{% enddocs %}

{% docs sav_Borough %}
Borough
{% enddocs %}

{% docs sav_Borough_Ward %}
Borough Ward
{% enddocs %}

{% docs sav_City %}
City
{% enddocs %}

{% docs sav_City_Council_Commissioner_District %}
City Council Commissioner District
{% enddocs %}

{% docs sav_City_Mayoral_District %}
City Mayoral District
{% enddocs %}

{% docs sav_City_Ward %}
City Ward
{% enddocs %}

{% docs sav_Hamlet_Community_Area %}
Hamlet Community Area
{% enddocs %}

{% docs sav_Proposed_City %}
Proposed City
{% enddocs %}

{% docs sav_Proposed_City_Commissioner_District %}
Proposed City Commissioner District
{% enddocs %}

{% docs sav_Town_Council %}
Town Council
{% enddocs %}

{% docs sav_Town_District %}
Town District
{% enddocs %}

{% docs sav_Town_Ward %}
Town Ward
{% enddocs %}

{% docs sav_Township %}
Township
{% enddocs %}

{% docs sav_Township_Ward %}
Township Ward
{% enddocs %}

{% docs sav_Village %}
Village
{% enddocs %}

{% docs sav_Village_Ward %}
Village Ward
{% enddocs %}

{% docs sav_Judicial_Appellate_District %}
Judicial Appellate District
{% enddocs %}

{% docs sav_Judicial_Chancery_Court %}
Judicial Chancery Court
{% enddocs %}

{% docs sav_Judicial_Circuit_Court_District %}
Judicial Circuit Court District
{% enddocs %}

{% docs sav_Judicial_County_Board_of_Review_District %}
Judicial County Board of Review District
{% enddocs %}

{% docs sav_Judicial_County_Court_District %}
Judicial County Court District
{% enddocs %}

{% docs sav_Judicial_District %}
Judicial District
{% enddocs %}

{% docs sav_Judicial_District_Court_District %}
Judicial District Court District
{% enddocs %}

{% docs sav_Judicial_Family_Court_District %}
Judicial Family Court District
{% enddocs %}

{% docs sav_Judicial_Jury_District %}
Judicial Jury District
{% enddocs %}

{% docs sav_Judicial_Justice_of_the_Peace %}
Judicial Justice of the Peace
{% enddocs %}

{% docs sav_Judicial_Juvenile_Court_District %}
Judicial Juvenile Court District
{% enddocs %}

{% docs sav_Judicial_Magistrate_Division %}
Judicial Magistrate Division
{% enddocs %}

{% docs sav_Judicial_Municipal_Court_District %}
Judicial Municipal Court District
{% enddocs %}

{% docs sav_Judicial_Sub_Circuit_District %}
Judicial Sub Circuit District
{% enddocs %}

{% docs sav_Judicial_Superior_Court_District %}
Judicial Superior Court District
{% enddocs %}

{% docs sav_Judicial_Supreme_Court_District %}
Judicial Supreme Court District
{% enddocs %}

{% docs sav_City_School_District %}
City School District
{% enddocs %}

{% docs sav_College_Board_District %}
College Board District
{% enddocs %}

{% docs sav_Community_College %}
Community College
{% enddocs %}

{% docs sav_Community_College_At_Large %}
Community College At Large
{% enddocs %}

{% docs sav_Community_College_Commissioner_District %}
Community College Commissioner District
{% enddocs %}

{% docs sav_Community_College_SubDistrict %}
Community College SubDistrict
{% enddocs %}

{% docs sav_County_Board_of_Education_District %}
County Board of Education District
{% enddocs %}

{% docs sav_County_Board_of_Education_SubDistrict %}
County Board of Education SubDistrict
{% enddocs %}

{% docs sav_County_Community_College_District %}
County Community College District
{% enddocs %}

{% docs sav_County_Superintendent_of_Schools_District %}
County Superintendent of Schools District
{% enddocs %}

{% docs sav_County_Unified_School_District %}
County Unified School District
{% enddocs %}

{% docs sav_Education_Commission_District %}
Education Commission District
{% enddocs %}

{% docs sav_Educational_Service_District %}
Educational Service District
{% enddocs %}

{% docs sav_Educational_Service_Subdistrict %}
Educational Service Subdistrict
{% enddocs %}

{% docs sav_Elementary_School_District %}
Elementary School District
{% enddocs %}

{% docs sav_Elementary_School_SubDistrict %}
Elementary School SubDistrict
{% enddocs %}

{% docs sav_Exempted_Village_School_District %}
Exempted Village School District
{% enddocs %}

{% docs sav_High_School_District %}
High School District
{% enddocs %}

{% docs sav_High_School_SubDistrict %}
High School SubDistrict
{% enddocs %}

{% docs sav_Proposed_Community_College %}
Proposed Community College
{% enddocs %}

{% docs sav_Proposed_Elementary_School_District %}
Proposed Elementary School District
{% enddocs %}

{% docs sav_Proposed_Unified_School_District %}
Proposed Unified School District
{% enddocs %}

{% docs sav_Regional_Office_of_Education_District %}
Regional Office of Education District
{% enddocs %}

{% docs sav_School_Board_District %}
School Board District
{% enddocs %}

{% docs sav_School_District %}
School District
{% enddocs %}

{% docs sav_School_District_Vocational %}
School District Vocational
{% enddocs %}

{% docs sav_School_Facilities_Improvement_District %}
School Facilities Improvement District
{% enddocs %}

{% docs sav_School_Subdistrict %}
School Subdistrict
{% enddocs %}

{% docs sav_Superintendent_of_Schools_District %}
Superintendent of Schools District
{% enddocs %}

{% docs sav_Unified_School_District %}
Unified School District
{% enddocs %}

{% docs sav_Unified_School_SubDistrict %}
Unified School SubDistrict
{% enddocs %}

{% docs sav_2024_Proposed_Congressional_District %}
2024 Proposed Congressional District
{% enddocs %}

{% docs sav_2024_Proposed_State_Senate_District %}
2024 Proposed State Senate District
{% enddocs %}

{% docs sav_2024_Proposed_State_House_District %}
2024 Proposed State House District
{% enddocs %}

{% docs sav_2024_Proposed_State_Legislative_District %}
2024 Proposed State Legislative District
{% enddocs %}

{% docs sav_2001_US_Congressional_District %}
2001 US Congressional District
{% enddocs %}

{% docs sav_2001_State_House_District %}
2001 State House District
{% enddocs %}

{% docs sav_2001_State_Legislative_District %}
2001 State Legislative District
{% enddocs %}

{% docs sav_2001_State_Senate_District %}
2001 State Senate District
{% enddocs %}

{% docs sav_2010_US_Congressional_District %}
2010 US Congressional District
{% enddocs %}

{% docs sav_2010_State_House_District %}
2010 State House District
{% enddocs %}

{% docs sav_2010_State_Legislative_District %}
2010 State Legislative District
{% enddocs %}

{% docs sav_2010_State_Senate_District %}
2010 State Senate District
{% enddocs %}

{% docs sav_4H_Livestock_District %}
4H Livestock District
{% enddocs %}

{% docs sav_Airport_District %}
Airport District
{% enddocs %}

{% docs sav_Annexation_District %}
Annexation District
{% enddocs %}

{% docs sav_Aquatic_Center_District %}
Aquatic Center District
{% enddocs %}

{% docs sav_Aquatic_District %}
Aquatic District
{% enddocs %}

{% docs sav_Assessment_District %}
Assessment District
{% enddocs %}

{% docs sav_Bay_Area_Rapid_Transit %}
Bay Area Rapid Transit
{% enddocs %}

{% docs sav_Board_of_Education_District %}
Board of Education District
{% enddocs %}

{% docs sav_Board_of_Education_SubDistrict %}
Board of Education SubDistrict
{% enddocs %}

{% docs sav_Bonds_District %}
Bonds District
{% enddocs %}

{% docs sav_Career_Center %}
Career Center
{% enddocs %}

{% docs sav_Cemetery_District %}
Cemetery District
{% enddocs %}

{% docs sav_Chemical_Control_District %}
Chemical Control District
{% enddocs %}

{% docs sav_Coast_Water_District %}
Coast Water District
{% enddocs %}

{% docs sav_Communications_District %}
Communications District
{% enddocs %}

{% docs sav_Community_Council_District %}
Community Council District
{% enddocs %}

{% docs sav_Community_Council_SubDistrict %}
Community Council SubDistrict
{% enddocs %}

{% docs sav_Community_Facilities_District %}
Community Facilities District
{% enddocs %}

{% docs sav_Community_Facilities_SubDistrict %}
Community Facilities SubDistrict
{% enddocs %}

{% docs sav_Community_Hospital_District %}
Community Hospital District
{% enddocs %}

{% docs sav_Community_Planning_Area %}
Community Planning Area
{% enddocs %}

{% docs sav_Community_Service_District %}
Community Service District
{% enddocs %}

{% docs sav_Community_Service_SubDistrict %}
Community Service SubDistrict
{% enddocs %}

{% docs sav_Congressional_Township %}
Congressional Township
{% enddocs %}

{% docs sav_Conservation_District %}
Conservation District
{% enddocs %}

{% docs sav_Conservation_SubDistrict %}
Conservation SubDistrict
{% enddocs %}

{% docs sav_Consolidated_Water_District %}
Consolidated Water District
{% enddocs %}

{% docs sav_Control_Zone_District %}
Control Zone District
{% enddocs %}

{% docs sav_Corrections_District %}
Corrections District
{% enddocs %}

{% docs sav_County_Fire_District %}
County Fire District
{% enddocs %}

{% docs sav_County_Hospital_District %}
County Hospital District
{% enddocs %}

{% docs sav_County_Legislative_District %}
County Legislative District
{% enddocs %}

{% docs sav_County_Library_District %}
County Library District
{% enddocs %}

{% docs sav_County_Memorial_District %}
County Memorial District
{% enddocs %}

{% docs sav_County_Paramedic_District %}
County Paramedic District
{% enddocs %}

{% docs sav_County_Service_Area %}
County Service Area
{% enddocs %}

{% docs sav_County_Service_Area_SubDistrict %}
County Service Area SubDistrict
{% enddocs %}

{% docs sav_County_Sewer_District %}
County Sewer District
{% enddocs %}

{% docs sav_County_Water_District %}
County Water District
{% enddocs %}

{% docs sav_County_Water_Landowner_District %}
County Water Landowner District
{% enddocs %}

{% docs sav_County_Water_SubDistrict %}
County Water SubDistrict
{% enddocs %}

{% docs sav_District_Attorney %}
District Attorney
{% enddocs %}

{% docs sav_Drainage_District %}
Drainage District
{% enddocs %}

{% docs sav_Election_Commissioner_District %}
Election Commissioner District
{% enddocs %}

{% docs sav_Emergency_Communication_911_District %}
Emergency Communication 911 District
{% enddocs %}

{% docs sav_Emergency_Communication_911_SubDistrict %}
Emergency Communication 911 SubDistrict
{% enddocs %}

{% docs sav_Enterprise_Zone_District %}
Enterprise Zone District
{% enddocs %}

{% docs sav_EXT_District %}
EXT District
{% enddocs %}

{% docs sav_Facilities_Improvement_District %}
Facilities Improvement District
{% enddocs %}

{% docs sav_Fire_District %}
Fire District
{% enddocs %}

{% docs sav_Fire_Maintenance_District %}
Fire Maintenance District
{% enddocs %}

{% docs sav_Fire_Protection_District %}
Fire Protection District
{% enddocs %}

{% docs sav_Fire_Protection_SubDistrict %}
Fire Protection SubDistrict
{% enddocs %}

{% docs sav_Fire_Protection_Tax_Measure_District %}
Fire Protection Tax Measure District
{% enddocs %}

{% docs sav_Fire_Service_Area_District %}
Fire Service Area District
{% enddocs %}

{% docs sav_Fire_SubDistrict %}
Fire SubDistrict
{% enddocs %}

{% docs sav_Flood_Control_Zone %}
Flood Control Zone
{% enddocs %}

{% docs sav_Forest_Preserve %}
Forest Preserve
{% enddocs %}

{% docs sav_Garbage_District %}
Garbage District
{% enddocs %}

{% docs sav_Geological_Hazard_Abatement_District %}
Geological Hazard Abatement District
{% enddocs %}

{% docs sav_Health_District %}
Health District
{% enddocs %}

{% docs sav_Hospital_District %}
Hospital District
{% enddocs %}

{% docs sav_Hospital_SubDistrict %}
Hospital SubDistrict
{% enddocs %}

{% docs sav_Improvement_Landowner_District %}
Improvement Landowner District
{% enddocs %}

{% docs sav_Independent_Fire_District %}
Independent Fire District
{% enddocs %}

{% docs sav_Irrigation_District %}
Irrigation District
{% enddocs %}

{% docs sav_Irrigation_SubDistrict %}
Irrigation SubDistrict
{% enddocs %}

{% docs sav_Island %}
Island
{% enddocs %}

{% docs sav_Land_Commission %}
Land Commission
{% enddocs %}

{% docs sav_Landscaping_and_Lighting_Assessment_District %}
Landscaping and Lighting Assessment District
{% enddocs %}

{% docs sav_Law_Enforcement_District %}
Law Enforcement District
{% enddocs %}

{% docs sav_Learning_Community_Coordinating_Council_District %}
Learning Community Coordinating Council District
{% enddocs %}

{% docs sav_Levee_District %}
Levee District
{% enddocs %}

{% docs sav_Levee_Reconstruction_Assesment_District %}
Levee Reconstruction Assesment District
{% enddocs %}

{% docs sav_Library_District %}
Library District
{% enddocs %}

{% docs sav_Library_Services_District %}
Library Services District
{% enddocs %}

{% docs sav_Library_SubDistrict %}
Library SubDistrict
{% enddocs %}

{% docs sav_Lighting_District %}
Lighting District
{% enddocs %}

{% docs sav_Local_Hospital_District %}
Local Hospital District
{% enddocs %}

{% docs sav_Local_Park_District %}
Local Park District
{% enddocs %}

{% docs sav_Maintenance_District %}
Maintenance District
{% enddocs %}

{% docs sav_Master_Plan_District %}
Master Plan District
{% enddocs %}

{% docs sav_Memorial_District %}
Memorial District
{% enddocs %}

{% docs sav_Metro_Service_District %}
Metro Service District
{% enddocs %}

{% docs sav_Metro_Service_Subdistrict %}
Metro Service Subdistrict
{% enddocs %}

{% docs sav_Metro_Transit_District %}
Metro Transit District
{% enddocs %}

{% docs sav_Metropolitan_Water_District %}
Metropolitan Water District
{% enddocs %}

{% docs sav_Middle_School_District %}
Middle School District
{% enddocs %}

{% docs sav_Mosquito_Abatement_District %}
Mosquito Abatement District
{% enddocs %}

{% docs sav_Mountain_Water_District %}
Mountain Water District
{% enddocs %}

{% docs sav_Multi_township_Assessor %}
Multi township Assessor
{% enddocs %}

{% docs sav_Municipal_Advisory_Council_District %}
Municipal Advisory Council District
{% enddocs %}

{% docs sav_Municipal_Utility_District %}
Municipal Utility District
{% enddocs %}

{% docs sav_Municipal_Utility_SubDistrict %}
Municipal Utility SubDistrict
{% enddocs %}

{% docs sav_Municipal_Water_District %}
Municipal Water District
{% enddocs %}

{% docs sav_Municipal_Water_SubDistrict %}
Municipal Water SubDistrict
{% enddocs %}

{% docs sav_Museum_District %}
Museum District
{% enddocs %}

{% docs sav_Northeast_Soil_and_Water_District %}
Northeast Soil and Water District
{% enddocs %}

{% docs sav_Open_Space_District %}
Open Space District
{% enddocs %}

{% docs sav_Open_Space_SubDistrict %}
Open Space SubDistrict
{% enddocs %}

{% docs sav_Other %}
Other
{% enddocs %}

{% docs sav_Paramedic_District %}
Paramedic District
{% enddocs %}

{% docs sav_Park_Commissioner_District %}
Park Commissioner District
{% enddocs %}

{% docs sav_Park_District %}
Park District
{% enddocs %}

{% docs sav_Park_SubDistrict %}
Park SubDistrict
{% enddocs %}

{% docs sav_Planning_Area_District %}
Planning Area District
{% enddocs %}

{% docs sav_Police_District %}
Police District
{% enddocs %}

{% docs sav_Port_District %}
Port District
{% enddocs %}

{% docs sav_Port_SubDistrict %}
Port SubDistrict
{% enddocs %}

{% docs sav_Power_District %}
Power District
{% enddocs %}

{% docs sav_Proposed_District %}
Proposed District
{% enddocs %}

{% docs sav_Proposed_Fire_District %}
Proposed Fire District
{% enddocs %}

{% docs sav_Public_Airport_District %}
Public Airport District
{% enddocs %}

{% docs sav_Public_Regulation_Commission %}
Public Regulation Commission
{% enddocs %}

{% docs sav_Public_Service_Commission_District %}
Public Service Commission District
{% enddocs %}

{% docs sav_Public_Utility_District %}
Public Utility District
{% enddocs %}

{% docs sav_Public_Utility_SubDistrict %}
Public Utility SubDistrict
{% enddocs %}

{% docs sav_Rapid_Transit_District %}
Rapid Transit District
{% enddocs %}

{% docs sav_Rapid_Transit_SubDistrict %}
Rapid Transit SubDistrict
{% enddocs %}

{% docs sav_Reclamation_District %}
Reclamation District
{% enddocs %}

{% docs sav_Recreation_District %}
Recreation District
{% enddocs %}

{% docs sav_Recreational_SubDistrict %}
Recreational SubDistrict
{% enddocs %}

{% docs sav_Resort_Improvement_District %}
Resort Improvement District
{% enddocs %}

{% docs sav_Resource_Conservation_District %}
Resource Conservation District
{% enddocs %}

{% docs sav_River_Water_District %}
River Water District
{% enddocs %}

{% docs sav_Road_Maintenance_District %}
Road Maintenance District
{% enddocs %}

{% docs sav_Rural_Service_District %}
Rural Service District
{% enddocs %}

{% docs sav_Sanitary_District %}
Sanitary District
{% enddocs %}

{% docs sav_Sanitary_SubDistrict %}
Sanitary SubDistrict
{% enddocs %}

{% docs sav_Service_Area_District %}
Service Area District
{% enddocs %}

{% docs sav_Sewer_District %}
Sewer District
{% enddocs %}

{% docs sav_Sewer_Maintenance_District %}
Sewer Maintenance District
{% enddocs %}

{% docs sav_Sewer_SubDistrict %}
Sewer SubDistrict
{% enddocs %}

{% docs sav_Snow_Removal_District %}
Snow Removal District
{% enddocs %}

{% docs sav_Soil_and_Water_District %}
Soil and Water District
{% enddocs %}

{% docs sav_Soil_and_Water_District_At_Large %}
Soil and Water District At Large
{% enddocs %}

{% docs sav_Special_Reporting_District %}
Special Reporting District
{% enddocs %}

{% docs sav_Special_Tax_District %}
Special Tax District
{% enddocs %}

{% docs sav_State_Board_of_Equalization %}
State Board of Equalization
{% enddocs %}

{% docs sav_Storm_Water_District %}
Storm Water District
{% enddocs %}

{% docs sav_Street_Lighting_District %}
Street Lighting District
{% enddocs %}

{% docs sav_Transit_District %}
Transit District
{% enddocs %}

{% docs sav_Transit_SubDistrict %}
Transit SubDistrict
{% enddocs %}

{% docs sav_TriCity_Service_District %}
TriCity Service District
{% enddocs %}

{% docs sav_TV_Translator_District %}
TV Translator District
{% enddocs %}

{% docs sav_Unincorporated_District %}
Unincorporated District
{% enddocs %}

{% docs sav_Unincorporated_Park_District %}
Unincorporated Park District
{% enddocs %}

{% docs sav_Unprotected_Fire_District %}
Unprotected Fire District
{% enddocs %}

{% docs sav_Ute_Creek_Soil_District %}
Ute Creek Soil District
{% enddocs %}

{% docs sav_Vector_Control_District %}
Vector Control District
{% enddocs %}

{% docs sav_Vote_By_Mail_Area %}
Vote By Mail Area
{% enddocs %}

{% docs sav_Wastewater_District %}
Wastewater District
{% enddocs %}

{% docs sav_Water_Agency %}
Water Agency
{% enddocs %}

{% docs sav_Water_Agency_SubDistrict %}
Water Agency SubDistrict
{% enddocs %}

{% docs sav_Water_Conservation_District %}
Water Conservation District
{% enddocs %}

{% docs sav_Water_Conservation_SubDistrict %}
Water Conservation SubDistrict
{% enddocs %}

{% docs sav_Water_Control_Water_Conservation %}
Water Control Water Conservation
{% enddocs %}

{% docs sav_Water_Control_Water_Conservation_SubDistrict %}
Water Control Water Conservation SubDistrict
{% enddocs %}

{% docs sav_Water_District %}
Water District
{% enddocs %}

{% docs sav_Water_Public_Utility_District %}
Water Public Utility District
{% enddocs %}

{% docs sav_Water_Public_Utility_Subdistrict %}
Water Public Utility Subdistrict
{% enddocs %}

{% docs sav_Water_Replacement_District %}
Water Replacement District
{% enddocs %}

{% docs sav_Water_Replacement_SubDistrict %}
Water Replacement SubDistrict
{% enddocs %}

{% docs sav_Water_SubDistrict %}
Water SubDistrict
{% enddocs %}

{% docs sav_Weed_District %}
Weed District
{% enddocs %}

{% docs sav_ConsumerData_Time_Zone %}
Time zone in which the household is located.
{% enddocs %}

{% docs sav_ConsumerData_RUS_Code %}
Rural-Urban-Suburban
{% enddocs %}

{% docs sav_ConsumerData_Length_Of_Residence_Code %}
Length of residence at this address in years. Modeled determined by the first year listed in the telephone directory or registration lists. This element is fed by Purchase Date of Home, along with other contributor's data such as Survey data, Public data, Self-reported data, Warranty registrations, Buying activity, Online surveys and registrations, Magazine subscriptions
{% enddocs %}

{% docs sav_Residence_Addresses_Density %}
Population-density code for the voter's residence area.
{% enddocs %}

{% docs sav_Designated_Market_Area_DMA %}
Designated Market Area DMA
{% enddocs %}

{% docs sav_Voters_VotingPerformanceEvenYearGeneral %}
Percentage of Even Year General Elections voted
{% enddocs %}

{% docs sav_Voters_VotingPerformanceEvenYearPrimary %}
Percentage of Even Year Primary Elections voted
{% enddocs %}

{% docs sav_Voters_VotingPerformanceEvenYearGeneralAndPrimary %}
Percentage of Even Year General and Primary Elections voted
{% enddocs %}

{% docs sav_Voters_VotingPerformanceMinorElection %}
Percentage of Minor Elections voted
{% enddocs %}

{% docs sav_General_2030 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2030 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2030 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2029 %}
Y or null
{% enddocs %}

{% docs sav_General_2028 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2028 %}
Y or null
{% enddocs %}

{% docs sav_PresidentialPrimary_2028 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2028 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2027 %}
Y or null
{% enddocs %}

{% docs sav_General_2026 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2026 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2026 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2025 %}
Y or null
{% enddocs %}

{% docs sav_General_2024 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2024 %}
Y or null
{% enddocs %}

{% docs sav_PresidentialPrimary_2024 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2024 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2023 %}
Y or null
{% enddocs %}

{% docs sav_General_2022 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2022 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2022 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2021 %}
Y or null
{% enddocs %}

{% docs sav_General_2020 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2020 %}
Y or null
{% enddocs %}

{% docs sav_PresidentialPrimary_2020 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2020 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2019 %}
Y or null
{% enddocs %}

{% docs sav_General_2018 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2018 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2018 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2017 %}
Y or null
{% enddocs %}

{% docs sav_General_2016 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2016 %}
Y or null
{% enddocs %}

{% docs sav_PresidentialPrimary_2016 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2016 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2015 %}
Y or null
{% enddocs %}

{% docs sav_General_2014 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2014 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2014 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2013 %}
Y or null
{% enddocs %}

{% docs sav_General_2012 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2012 %}
Y or null
{% enddocs %}

{% docs sav_PresidentialPrimary_2012 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2012 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2011 %}
Y or null
{% enddocs %}

{% docs sav_General_2010 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2010 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2010 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2009 %}
Y or null
{% enddocs %}

{% docs sav_General_2008 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2008 %}
Y or null
{% enddocs %}

{% docs sav_PresidentialPrimary_2008 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2008 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2007 %}
Y or null
{% enddocs %}

{% docs sav_General_2006 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2006 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2006 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2005 %}
Y or null
{% enddocs %}

{% docs sav_General_2004 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2004 %}
Y or null
{% enddocs %}

{% docs sav_PresidentialPrimary_2004 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2004 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2003 %}
Y or null
{% enddocs %}

{% docs sav_General_2002 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2002 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2002 %}
Y or null
{% enddocs %}

{% docs sav_AnyElection_2001 %}
Y or null
{% enddocs %}

{% docs sav_General_2000 %}
Y or null
{% enddocs %}

{% docs sav_Primary_2000 %}
Y or null
{% enddocs %}

{% docs sav_PresidentialPrimary_2000 %}
Y or null
{% enddocs %}

{% docs sav_OtherElection_2000 %}
Y or null
{% enddocs %}

{% docs sav_state_postal_code %}
State postal code
{% enddocs %}

{% docs sav_hf_activism %}
Haystaq issue flag: activism
{% enddocs %}

{% docs sav_hf_attends_church %}
Haystaq issue flag: attends church
{% enddocs %}

{% docs sav_hf_candidate_mail %}
Haystaq issue flag: candidate mail
{% enddocs %}

{% docs sav_hf_charity_giving %}
Haystaq issue flag: charity giving
{% enddocs %}

{% docs sav_hf_community_integration %}
Haystaq issue flag: community integration
{% enddocs %}

{% docs sav_hf_dating %}
Haystaq issue flag: dating
{% enddocs %}

{% docs sav_hf_dropoff_fill %}
Haystaq issue flag: dropoff fill
{% enddocs %}

{% docs sav_hf_most_important_policy_item %}
Haystaq issue flag: most important policy item
{% enddocs %}

{% docs sav_hf_political_donations %}
Haystaq issue flag: political donations
{% enddocs %}

{% docs sav_hf_religion %}
Haystaq issue flag: religion
{% enddocs %}

{% docs sav_hf_responsiveness_email %}
Haystaq issue flag: responsiveness email
{% enddocs %}

{% docs sav_hf_responsiveness_live %}
Haystaq issue flag: responsiveness live
{% enddocs %}

{% docs sav_hf_responsiveness_sms %}
Haystaq issue flag: responsiveness sms
{% enddocs %}

{% docs sav_hf_age_limit %}
Haystaq issue flag: age limit
{% enddocs %}

{% docs sav_hf_candidate_female_prefer %}
Haystaq issue flag: candidate female prefer
{% enddocs %}

{% docs sav_hf_candidate_minority_prefer %}
Haystaq issue flag: candidate minority prefer
{% enddocs %}

{% docs sav_hf_consumer_value %}
Haystaq issue flag: consumer value
{% enddocs %}

{% docs sav_hf_electric_vehicle_buyer %}
Haystaq issue flag: electric vehicle buyer
{% enddocs %}

{% docs sav_hf_home_buyer_any %}
Haystaq issue flag: home buyer any
{% enddocs %}

{% docs sav_hf_home_buyer_new %}
Haystaq issue flag: home buyer new
{% enddocs %}

{% docs sav_hf_rideshare_user %}
Haystaq issue flag: rideshare user
{% enddocs %}

{% docs sav_hf_solar_panel_buyer %}
Haystaq issue flag: solar panel buyer
{% enddocs %}

{% docs sav_hf_tv_viewer %}
Haystaq issue flag: tv viewer
{% enddocs %}

{% docs sav_hf_vaping_user %}
Haystaq issue flag: vaping user
{% enddocs %}

{% docs sav_hf_affordability_changed_what_you_buy %}
Haystaq issue flag: affordability changed what you buy
{% enddocs %}

{% docs sav_hf_affordable_housing_gov %}
Haystaq issue flag: affordable housing gov
{% enddocs %}

{% docs sav_hf_amazon_worker_treatment %}
Haystaq issue flag: amazon worker treatment
{% enddocs %}

{% docs sav_hf_attitude_towards_unions %}
Haystaq issue flag: attitude towards unions
{% enddocs %}

{% docs sav_hf_capitalism_believe %}
Haystaq issue flag: capitalism believe
{% enddocs %}

{% docs sav_hf_crypto_buyer %}
Haystaq issue flag: crypto buyer
{% enddocs %}

{% docs sav_hf_crypto_restrictions %}
Haystaq issue flag: crypto restrictions
{% enddocs %}

{% docs sav_hf_economic_anxiety %}
Haystaq issue flag: economic anxiety
{% enddocs %}

{% docs sav_hf_economic_despondency %}
Haystaq issue flag: economic despondency
{% enddocs %}

{% docs sav_hf_gentrification %}
Haystaq issue flag: gentrification
{% enddocs %}

{% docs sav_hf_gig_work_benefits %}
Haystaq issue flag: gig work benefits
{% enddocs %}

{% docs sav_hf_gig_worker %}
Haystaq issue flag: gig worker
{% enddocs %}

{% docs sav_hf_gig_worker_historical %}
Haystaq issue flag: gig worker historical
{% enddocs %}

{% docs sav_hf_income_inequality %}
Haystaq issue flag: income inequality
{% enddocs %}

{% docs sav_hf_inflation_fault %}
Haystaq issue flag: inflation fault
{% enddocs %}

{% docs sav_hf_infrastructure_funding %}
Haystaq issue flag: infrastructure funding
{% enddocs %}

{% docs sav_hf_job_seeker %}
Haystaq issue flag: job seeker
{% enddocs %}

{% docs sav_hf_tax_cuts %}
Haystaq issue flag: tax cuts
{% enddocs %}

{% docs sav_hf_charter_schools %}
Haystaq issue flag: charter schools
{% enddocs %}

{% docs sav_hf_critical_race_theory_books %}
Haystaq issue flag: critical race theory books
{% enddocs %}

{% docs sav_hf_school_choice %}
Haystaq issue flag: school choice
{% enddocs %}

{% docs sav_hf_school_funding %}
Haystaq issue flag: school funding
{% enddocs %}

{% docs sav_hf_teachers_union %}
Haystaq issue flag: teachers union
{% enddocs %}

{% docs sav_hf_climate_change %}
Haystaq issue flag: climate change
{% enddocs %}

{% docs sav_hf_gas_tax %}
Haystaq issue flag: gas tax
{% enddocs %}

{% docs sav_hf_green_new_deal %}
Haystaq issue flag: green new deal
{% enddocs %}

{% docs sav_hf_pipeline_fracking %}
Haystaq issue flag: pipeline fracking
{% enddocs %}

{% docs sav_hf_abortion_pro %}
Haystaq issue flag: abortion pro
{% enddocs %}

{% docs sav_hf_general_anti_vax_pro_vax %}
Haystaq issue flag: general anti vax pro vax
{% enddocs %}

{% docs sav_hf_medicaid_expansion %}
Haystaq issue flag: medicaid expansion
{% enddocs %}

{% docs sav_hf_medicare_for_all %}
Haystaq issue flag: medicare for all
{% enddocs %}

{% docs sav_hf_obamacare_aca %}
Haystaq issue flag: obamacare aca
{% enddocs %}

{% docs sav_hf_area_identity %}
Haystaq issue flag: area identity
{% enddocs %}

{% docs sav_hf_gamer %}
Haystaq issue flag: gamer
{% enddocs %}

{% docs sav_hf_listen_podcaster %}
Haystaq issue flag: listen podcaster
{% enddocs %}

{% docs sav_hf_listen_podcaster_leaning %}
Haystaq issue flag: listen podcaster leaning
{% enddocs %}

{% docs sav_hf_news_source %}
Haystaq issue flag: news source
{% enddocs %}

{% docs sav_hf_podcast_listener %}
Haystaq issue flag: podcast listener
{% enddocs %}

{% docs sav_hf_social_media_user %}
Haystaq issue flag: social media user
{% enddocs %}

{% docs sav_hf_tv_news_source_most_trusted %}
Haystaq issue flag: tv news source most trusted
{% enddocs %}

{% docs sav_hf_china_foreign_policy %}
Haystaq issue flag: china foreign policy
{% enddocs %}

{% docs sav_hf_defense_spending %}
Haystaq issue flag: defense spending
{% enddocs %}

{% docs sav_hf_immigration_illegal %}
Haystaq issue flag: immigration illegal
{% enddocs %}

{% docs sav_hf_israel_committing_genocide %}
Haystaq issue flag: israel committing genocide
{% enddocs %}

{% docs sav_hf_israel_military_actions %}
Haystaq issue flag: israel military actions
{% enddocs %}

{% docs sav_hf_mass_deportations %}
Haystaq issue flag: mass deportations
{% enddocs %}

{% docs sav_hf_mexican_wall %}
Haystaq issue flag: mexican wall
{% enddocs %}

{% docs sav_hf_military_family_relationship %}
Haystaq issue flag: military family relationship
{% enddocs %}

{% docs sav_hf_military_family_self %}
Haystaq issue flag: military family self
{% enddocs %}

{% docs sav_hf_super_power_policy %}
Haystaq issue flag: super power policy
{% enddocs %}

{% docs sav_hf_2025_government_shutdown_fault %}
Haystaq issue flag: 2025 government shutdown fault
{% enddocs %}

{% docs sav_hf_artificial_intelligence %}
Haystaq issue flag: artificial intelligence
{% enddocs %}

{% docs sav_hf_christian_values %}
Haystaq issue flag: christian values
{% enddocs %}

{% docs sav_hf_college_admissions_race %}
Haystaq issue flag: college admissions race
{% enddocs %}

{% docs sav_hf_concerned_job_loss_due_to_ai %}
Haystaq issue flag: concerned job loss due to ai
{% enddocs %}

{% docs sav_hf_conspiracy %}
Haystaq issue flag: conspiracy
{% enddocs %}

{% docs sav_hf_dei %}
Haystaq issue flag: dei
{% enddocs %}

{% docs sav_hf_doge %}
Haystaq issue flag: doge
{% enddocs %}

{% docs sav_hf_domestic_deployment_of_troops %}
Haystaq issue flag: domestic deployment of troops
{% enddocs %}

{% docs sav_hf_epstein_files %}
Haystaq issue flag: epstein files
{% enddocs %}

{% docs sav_hf_ice_actions %}
Haystaq issue flag: ice actions
{% enddocs %}

{% docs sav_hf_jan_6th_pardons %}
Haystaq issue flag: jan 6th pardons
{% enddocs %}

{% docs sav_hf_police_trust %}
Haystaq issue flag: police trust
{% enddocs %}

{% docs sav_hf_political_troll %}
Haystaq issue flag: political troll
{% enddocs %}

{% docs sav_hf_same_sex_marriage %}
Haystaq issue flag: same sex marriage
{% enddocs %}

{% docs sav_hf_snap %}
Haystaq issue flag: snap
{% enddocs %}

{% docs sav_hf_traditional_gender_roles %}
Haystaq issue flag: traditional gender roles
{% enddocs %}

{% docs sav_hf_trans_athlete %}
Haystaq issue flag: trans athlete
{% enddocs %}

{% docs sav_hf_violent_crime %}
Haystaq issue flag: violent crime
{% enddocs %}

{% docs sav_hf_voting_fraud_concern %}
Haystaq issue flag: voting fraud concern
{% enddocs %}

{% docs sav_hf_campaign_finance_reform %}
Haystaq issue flag: campaign finance reform
{% enddocs %}

{% docs sav_hf_casino %}
Haystaq issue flag: casino
{% enddocs %}

{% docs sav_hf_death_penalty %}
Haystaq issue flag: death penalty
{% enddocs %}

{% docs sav_hf_gun_control %}
Haystaq issue flag: gun control
{% enddocs %}

{% docs sav_hf_marijuana_legal %}
Haystaq issue flag: marijuana legal
{% enddocs %}

{% docs sav_hf_online_gambling %}
Haystaq issue flag: online gambling
{% enddocs %}

{% docs sav_hf_public_transit %}
Haystaq issue flag: public transit
{% enddocs %}

{% docs sav_hf_regulations %}
Haystaq issue flag: regulations
{% enddocs %}

{% docs sav_hf_stadium_public_financing %}
Haystaq issue flag: stadium public financing
{% enddocs %}

{% docs sav_hf_likely_ev %}
Haystaq issue flag: likely ev
{% enddocs %}

{% docs sav_hf_likely_mid_term_voter %}
Haystaq issue flag: likely mid term voter
{% enddocs %}

{% docs sav_hf_likely_presidential_voter %}
Haystaq issue flag: likely presidential voter
{% enddocs %}

{% docs sav_hf_likely_presidential_voter_and_likely_ev %}
Haystaq modeled flag - Likely Presidential Voter And Likely Ev; Combination Flags: High Turnout and Likely Early Voters; flag=1 means condition met.
{% enddocs %}

{% docs sav_hf_likely_presidential_voter_and_likely_vbm %}
Haystaq issue flag: likely presidential voter and likely vbm
{% enddocs %}

{% docs sav_hf_likely_presidential_voter_and_unlikely_ev %}
Haystaq modeled flag - Likely Presidential Voter And Unlikely Ev; Combination Flags: Low Turnout and Unlikely Early Voters; flag=1 means condition met.
{% enddocs %}

{% docs sav_hf_likely_presidential_voter_and_unlikely_vbm %}
Haystaq issue flag: likely presidential voter and unlikely vbm
{% enddocs %}

{% docs sav_hf_likely_vbm %}
Haystaq issue flag: likely vbm
{% enddocs %}

{% docs sav_hf_unlikely_presidential_voter_and_likely_vbm %}
Haystaq issue flag: unlikely presidential voter and likely vbm
{% enddocs %}

{% docs sav_hf_felon_voting %}
Haystaq issue flag: felon voting
{% enddocs %}

{% docs sav_hf_rank_choice_voting %}
Haystaq issue flag: rank choice voting
{% enddocs %}

{% docs sav_hf_redistricting %}
Haystaq issue flag: redistricting
{% enddocs %}

{% docs sav_hf_alien_disclosure_govt %}
Haystaq issue flag: alien disclosure govt
{% enddocs %}

{% docs sav_hf_autonomous_vehicles %}
Haystaq issue flag: autonomous vehicles
{% enddocs %}

{% docs sav_hf_civil_liberties %}
Haystaq issue flag: civil liberties
{% enddocs %}

{% docs sav_hf_despondency %}
Haystaq issue flag: despondency
{% enddocs %}

{% docs sav_hf_family_medical_leave %}
Haystaq issue flag: family medical leave
{% enddocs %}

{% docs sav_hf_free_community_college %}
Haystaq issue flag: free community college
{% enddocs %}

{% docs sav_hf_insurance_of_last_resort %}
Haystaq issue flag: insurance of last resort
{% enddocs %}

{% docs sav_hf_jobs_guarantee %}
Haystaq issue flag: jobs guarantee
{% enddocs %}

{% docs sav_hf_min_wage_15 %}
Haystaq issue flag: min wage 15
{% enddocs %}

{% docs sav_hf_opioid_crisis %}
Haystaq issue flag: opioid crisis
{% enddocs %}

{% docs sav_hf_sell_federal_lands %}
Haystaq issue flag: sell federal lands
{% enddocs %}

{% docs sav_hf_social_media_truth_vs_speech %}
Haystaq issue flag: social media truth vs speech
{% enddocs %}

{% docs sav_hf_social_security_tax_increase %}
Haystaq issue flag: social security tax increase
{% enddocs %}

{% docs sav_hf_state_level_fema %}
Haystaq issue flag: state level fema
{% enddocs %}

{% docs sav_hf_tribalism %}
Haystaq issue flag: tribalism
{% enddocs %}

{% docs sav_hf_trust_science %}
Haystaq issue flag: trust science
{% enddocs %}

{% docs sav_hf_united_healthcare %}
Haystaq issue flag: united healthcare
{% enddocs %}

{% docs sav_hf_voting_fraud %}
Haystaq issue flag: voting fraud
{% enddocs %}

{% docs sav_hf_wealth_acquired %}
Haystaq issue flag: wealth acquired
{% enddocs %}

{% docs sav_hs_activism %}
Haystaq modeled score (0-100) - Activism; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_attends_church_frequently %}
Haystaq modeled score (0-100) - Attends Church Frequently; survey question: "How often do you attend church, synagogue or mosque?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_attends_church_never %}
Haystaq modeled score (0-100) - Attends Church Never; survey question: "How often do you attend church, synagogue or mosque?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_candidate_mail_readership_do_not_read %}
Haystaq issue score (0-100): candidate mail readership do not read
{% enddocs %}

{% docs sav_hs_candidate_mail_readership_read_carefully %}
Haystaq issue score (0-100): candidate mail readership read carefully
{% enddocs %}

{% docs sav_hs_charity_giving_enviro_cause %}
Haystaq modeled score (0-100) - Charity Giving Enviro Cause; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_charity_giving_international_aid %}
Haystaq modeled score (0-100) - Charity Giving International Aid; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_charity_giving_performing_arts %}
Haystaq modeled score (0-100) - Charity Giving Performing Arts; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_charity_giving_relig_cause %}
Haystaq modeled score (0-100) - Charity Giving Relig Cause; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_charity_giving_vet_cause %}
Haystaq modeled score (0-100) - Charity Giving Vet Cause; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_community_not_integrated %}
Haystaq issue score (0-100): community not integrated
{% enddocs %}

{% docs sav_hs_community_very_integrated %}
Haystaq issue score (0-100): community very integrated
{% enddocs %}

{% docs sav_hs_dating_optimistic %}
Haystaq issue score (0-100): dating optimistic
{% enddocs %}

{% docs sav_hs_dating_pessimistic %}
Haystaq issue score (0-100): dating pessimistic
{% enddocs %}

{% docs sav_hs_dropoff_fill_entire_ballot %}
Haystaq modeled score (0-100) - Dropoff Fill Entire Ballot; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_dropoff_fill_only_top %}
Haystaq modeled score (0-100) - Dropoff Fill Only Top; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_most_important_policy_item_economics %}
Haystaq modeled score (0-100) - Most Important Policy Item Economics; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_most_important_policy_item_environment %}
Haystaq modeled score (0-100) - Most Important Policy Item Environment; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_most_important_policy_item_help_people %}
Haystaq modeled score (0-100) - Most Important Policy Item Help People; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_most_important_policy_item_religious_values %}
Haystaq modeled score (0-100) - Most Important Policy Item Religious Values; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_most_important_policy_keep_safe %}
Haystaq modeled score (0-100) - Most Important Policy Keep Safe; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_political_donations_likely %}
Haystaq modeled score (0-100) - Political Donations Likely; survey question: "How likely are you to donate to a political campaign in the coming year?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_political_donations_unlikely %}
Haystaq modeled score (0-100) - Political Donations Unlikely; survey question: "How likely are you to donate to a political campaign in the coming year?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_religion_important %}
Haystaq modeled score (0-100) - Religion Important; survey question: "How important would you say religion is in your own life?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_religion_not_important %}
Haystaq modeled score (0-100) - Religion Not Important; survey question: "How important would you say religion is in your own life?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_responsiveness_email %}
Haystaq modeled score (0-100) - Responsiveness Email; Responsiveness: Respondents via email to the survet vs. those who we attempted to reach but who did not respond to the survey via email; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_responsiveness_live %}
Haystaq modeled score (0-100) - Responsiveness Live; Responsiveness: Respondents via live calling to the survey vs. those who we attempted to reach but who did not respond to the survey via live calling; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_responsiveness_sms %}
Haystaq modeled score (0-100) - Responsiveness Sms; Responsiveness: Respondents via texting to the surve vs. those who we attempted to reach but who did not respond to the survey via texting; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_age_limit_oppose %}
Haystaq modeled score (0-100) - Age Limit Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_age_limit_support %}
Haystaq modeled score (0-100) - Age Limit Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_candidate_female_do_not_prefer %}
Haystaq modeled score (0-100) - Candidate Female Do Not Prefer; survey question: "Thinking of candidates for political office, with all other factors being equal, do you prefer to support a woman over an equally-qualified man?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_candidate_female_prefer %}
Haystaq modeled score (0-100) - Candidate Female Prefer; survey question: "Thinking of candidates for political office, with all other factors being equal, do you prefer to support a woman over an equally-qualified man?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_candidate_minority_do_not_prefer %}
Haystaq modeled score (0-100) - Candidate Minority Do Not Prefer; survey question: "Thinking of candidates for political office, with all other factors being equal, do you prefer to support a non-white candidate over an equally-qualified white candidate?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_candidate_minority_prefer %}
Haystaq modeled score (0-100) - Candidate Minority Prefer; survey question: "Thinking of candidates for political office, with all other factors being equal, do you prefer to support a non-white candidate over an equally-qualified white candidate?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_consumer_value_brand_savings %}
Haystaq modeled score (0-100) - Consumer Value Brand Savings; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_consumer_value_environment %}
Haystaq modeled score (0-100) - Consumer Value Environment; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_consumer_value_low_cost %}
Haystaq modeled score (0-100) - Consumer Value Low Cost; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_electric_vehicle_likely_buyer %}
Haystaq modeled score (0-100) - Electric Vehicle Likely Buyer; survey question: "How likely are you to own or buy a fully electric vehicle in the next three years?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_electric_vehicle_not_likely %}
Haystaq modeled score (0-100) - Electric Vehicle Not Likely; survey question: "How likely are you to own or buy a fully electric vehicle in the next three years?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_any_home_buyer %}
Haystaq modeled score (0-100) - Any Home Buyer; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_new_home_buyer %}
Haystaq modeled score (0-100) - New Home Buyer; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_rideshare_user %}
Haystaq modeled score (0-100) - Rideshare User; survey question: "Do you often use a ride-sharing service like Uber or Lyft?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_rideshare_user_no %}
Haystaq modeled score (0-100) - Rideshare User No; survey question: "Do you often use a ride-sharing service like Uber or Lyft?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_solar_panel_buyer_no %}
Haystaq modeled score (0-100) - Solar Panel Buyer No; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_solar_panel_buyer_yes %}
Haystaq modeled score (0-100) - Solar Panel Buyer Yes; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_tv_viewer_free_streaming %}
Haystaq modeled score (0-100) - Tv Viewer Free Streaming; survey question: "broadcast, cable and satellite TV or Paid streaming services such as Netflix, Hulu, and Disney+ or free platforms like Youtube, TikTok and Twitch or do you typically not watch TV?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_tv_viewer_not_viewer %}
Haystaq modeled score (0-100) - Tv Viewer Not Viewer; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_tv_viewer_watch_any_tv %}
Haystaq modeled score (0-100) - Tv Viewer Watch Any Tv; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_tv_viewer_watch_paid_streaming %}
Haystaq modeled score (0-100) - Tv Viewer Watch Paid Streaming; survey question: "broadcast, cable and satellite TV or Paid streaming services such as Netflix, Hulu, and Disney+ or free platforms like Youtube, TikTok and Twitch or do you typically not watch TV?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_vaping_user_no %}
Haystaq modeled score (0-100) - Vaping User No; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_vaping_user_yes %}
Haystaq modeled score (0-100) - Vaping User Yes; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_affordability_changed_what_you_buy_no %}
Haystaq issue score (0-100): affordability changed what you buy no
{% enddocs %}

{% docs sav_hs_affordability_changed_what_you_buy_yes %}
Haystaq issue score (0-100): affordability changed what you buy yes
{% enddocs %}

{% docs sav_hs_affordable_housing_gov_has_role %}
Haystaq modeled score (0-100) - Affordable Housing Gov Has Role; survey question: "Thinking about the cost of housing, which comes closest to your view? Government officials should work to ensure that there is enough affordable housing, or the market alone should determine housing prices?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_affordable_housing_gov_no_role %}
Haystaq modeled score (0-100) - Affordable Housing Gov No Role; survey question: "Thinking about the cost of housing, which comes closest to your view? Government officials should work to ensure that there is enough affordable housing, or the market alone should determine housing prices?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_amazon_exploitative %}
Haystaq modeled score (0-100) - Amazon Exploitative; survey question: "Do you believe Amazon treats its warehouse workers and delivery drivers fairly? Do you think these are good jobs that pay well or that Amazon is exploitive with the demands it places on workers?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_amazon_good_jobs %}
Haystaq modeled score (0-100) - Amazon Good Jobs; survey question: "Do you believe Amazon treats its warehouse workers and delivery drivers fairly? Do you think these are good jobs that pay well or that Amazon is exploitive with the demands it places on workers?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_unions_beneficial %}
Haystaq modeled score (0-100) - Unions Beneficial; survey question: "Do you believe labor unions are mostly beneficial or mostly harmful?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_unions_not_beneficial %}
Haystaq modeled score (0-100) - Unions Not Beneficial; survey question: "Do you believe labor unions are mostly beneficial or mostly harmful?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_capitalism_believe_flawed %}
Haystaq modeled score (0-100) - Capitalism Believe Flawed; survey question: "Do you believe that our current capitalist economic system is thriving or failing? Do you believe: that the US economic system is fundamentally sound, that our system has minor flaws that can easily be fixed, that our economic system has major flaws that need large remedies, or that our economic system is fundamentally flawed and needs to be replaced?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_capitalism_believe_sound %}
Haystaq modeled score (0-100) - Capitalism Believe Sound; survey question: "Do you believe that our current capitalist economic system is thriving or failing? Do you believe: that the US economic system is fundamentally sound, that our system has minor flaws that can easily be fixed, that our economic system has major flaws that need large remedies, or that our economic system is fundamentally flawed and needs to be replaced?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_crypto_buyer_no %}
Haystaq modeled score (0-100) - Crypto Buyer No; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_crypto_buyer_yes %}
Haystaq modeled score (0-100) - Crypto Buyer Yes; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_crypto_increase_restrictions %}
Haystaq modeled score (0-100) - Crypto Increase Restrictions; survey question: "How do you feel about the government’s role in regulating crypto currencies like Bitcoin? Should the government have NO role in regulating crypto currency? Do you believe the current rules and regulations are generally sufficient? Do you believe the government should do more to protect the public? Or do you believe crypto currencies should be severely restricted or banned?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_crypto_reduce_leave_as_is %}
Haystaq modeled score (0-100) - Crypto Reduce Leave As Is; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_crypto_reduce_restrictions %}
Haystaq modeled score (0-100) - Crypto Reduce Restrictions; survey question: "How do you feel about the government’s role in regulating crypto currencies like Bitcoin? Should the government have NO role in regulating crypto currency? Do you believe the current rules and regulations are generally sufficient? Do you believe the government should do more to protect the public? Or do you believe crypto currencies should be severely restricted or banned?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_econ_anxiety_not_worried %}
Haystaq modeled score (0-100) - Econ Anxiety Not Worried; survey question: "How worried are you about things like your job security, having enough money for your retirement, or your childrens economic prospects?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_econ_anxiety_very_worried %}
Haystaq modeled score (0-100) - Econ Anxiety Very Worried; survey question: "How worried are you about things like your job security, having enough money for your retirement, or your childrens economic prospects?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_economic_despondency_ahead %}
Haystaq issue score (0-100): economic despondency ahead
{% enddocs %}

{% docs sav_hs_economic_despondency_far_behind %}
Haystaq issue score (0-100): economic despondency far behind
{% enddocs %}

{% docs sav_hs_gentrification_oppose %}
Haystaq modeled score (0-100) - Gentrification Oppose; survey question: "What is your opinion on gentrification, the process where cities and neighborhoods change as wealthier people move into poorer neighborhoods?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_gentrification_support %}
Haystaq modeled score (0-100) - Gentrification Support; survey question: "What is your opinion on gentrification, the process where cities and neighborhoods change as wealthier people move into poorer neighborhoods?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_gig_work_keep_contractor %}
Haystaq modeled score (0-100) - Gig Work Keep Contractor; survey question: "Should “gig economy” companies like Uber, Lyft and Instacart be allowed to continue to treat their workers as independent contractors or should they be required to treat them as employees with access to benefits. Independent Contractors, Employees with Benefits, Unsure"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_gig_work_make_employees %}
Haystaq modeled score (0-100) - Gig Work Make Employees; survey question: "Should “gig economy” companies like Uber, Lyft and Instacart be allowed to continue to treat their workers as independent contractors or should they be required to treat them as employees with access to benefits. Independent Contractors, Employees with Benefits, Unsure"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_gig_worker_ever %}
Haystaq modeled score (0-100) - Gig Worker Ever; survey question: "Have you ever worked for a “gig economy” company?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_gig_worker_now %}
Haystaq modeled score (0-100) - Gig Worker Now; survey question: "Have you ever worked for a “gig economy” company?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_gig_worker_unlikely %}
Haystaq modeled score (0-100) - Gig Worker Unlikely; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_income_inequality_no_issue %}
Haystaq modeled score (0-100) - Income Inequality No Issue; survey question: "How big a problem do you think income inequality is in the United States? Do you think income inequality is a big problem, somewhat of a problem, or not a problem at all?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_income_inequality_serious %}
Haystaq modeled score (0-100) - Income Inequality Serious; survey question: "How big a problem do you think income inequality is in the United States? Do you think income inequality is a big problem, somewhat of a problem, or not a problem at all?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_inflation_fault_biden %}
Haystaq modeled score (0-100) - Inflation Fault Biden; survey question: "What is most responsible for recent price increases and inflation? Is it the policies of the Biden Administration, actions by Corporate America, External Events like Covid-19 or is it recent increases to employee wages?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_inflation_fault_corporate_america %}
Haystaq modeled score (0-100) - Inflation Fault Corporate America; survey question: "What is most responsible for recent price increases and inflation? Is it the policies of the Biden Administration, actions by Corporate America, External Events like Covid-19 or is it recent increases to employee wages?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_inflation_fault_external_events %}
Haystaq modeled score (0-100) - Inflation Fault External Events; survey question: "What is most responsible for recent price increases and inflation? Is it the policies of the Biden Administration, actions by Corporate America, External Events like Covid-19 or is it recent increases to employee wages?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_infrastructure_funding_enough_spent %}
Haystaq modeled score (0-100) - Infrastructure Funding Enough Spent; survey question: "Would you support increasing funding for infrastructure projects like bridges and roads even if it meant your taxes would be slightly higher, or do you think that enough money is already spent on infrastructure projects?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_infrastructure_funding_fund_more %}
Haystaq modeled score (0-100) - Infrastructure Funding Fund More; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_job_seeker_does_not_work %}
Haystaq modeled score (0-100) - Job Seeker Does Not Work; survey question: "Are you likely to look for a new job in the next six months?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_job_seeker_likely %}
Haystaq modeled score (0-100) - Job Seeker Likely; survey question: "Are you likely to look for a new job in the next six months?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_job_seeker_unlikely %}
Haystaq modeled score (0-100) - Job Seeker Unlikely; survey question: "Are you likely to look for a new job in the next six months?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_tax_cuts_oppose %}
Haystaq modeled score (0-100) - Tax Cuts Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_tax_cuts_support %}
Haystaq modeled score (0-100) - Tax Cuts Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_charter_schools_oppose %}
Haystaq modeled score (0-100) - Charter Schools Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_charter_schools_support %}
Haystaq modeled score (0-100) - Charter Schools Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_critical_race_theory_books_ban %}
Haystaq modeled score (0-100) - Critical Race Theory Books Ban; survey question: "Do you think it is ok to remove books from public school libraries based on content such as discussions of slavery or racial inequality, discussions of homosexual or transexual issues or criticisms of US history or historical figures?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_critical_race_theory_books_do_not_ban %}
Haystaq modeled score (0-100) - Critical Race Theory Books Do Not Ban; survey question: "Do you think it is ok to remove books from public school libraries based on content such as discussions of slavery or racial inequality, discussions of homosexual or transexual issues or criticisms of US history or historical figures?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_school_choice_oppose %}
Haystaq modeled score (0-100) - School Choice Oppose; survey question: "School choice allows parents to use the tax dollars associated with their child’s education to enroll them in the school of their choice, whether public or private. Do you agree more with supporters who say this gives parents the flexibility to choose the right schools for their children or with opponents who say that school choice subsidizes the wealthy while starving the schools that need funding the most. Support School Choice, Oppose School Choice, Unsure"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_school_choice_support %}
Haystaq modeled score (0-100) - School Choice Support; survey question: "School choice allows parents to use the tax dollars associated with their child’s education to enroll them in the school of their choice, whether public or private. Do you agree more with supporters who say this gives parents the flexibility to choose the right schools for their children or with opponents who say that school choice subsidizes the wealthy while starving the schools that need funding the most. Support School Choice, Oppose School Choice, Unsure"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_school_funding_less %}
Haystaq modeled score (0-100) - School Funding Less; survey question: "Would you support increasing funding for public education in your state or community even if it meant your taxes would be higher, or do you think that enough money is already spent on education and that schools need to spend better not more?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_school_funding_more %}
Haystaq modeled score (0-100) - School Funding More; survey question: "Would you support increasing funding for public education in your state or community even if it meant your taxes would be higher, or do you think that enough money is already spent on education and that schools need to spend better not more?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_teachers_union_negative %}
Haystaq modeled score (0-100) - Teachers Union Negative; survey question: "Do you think Teacher Unions have a positive impact on schools or a negative impact?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_teachers_union_positive %}
Haystaq modeled score (0-100) - Teachers Union Positive; survey question: "Do you think Teacher Unions have a positive impact on schools or a negative impact?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_climate_change_believer %}
Haystaq modeled score (0-100) - Climate Change Believer; survey question: "From what youve read and heard, is there solid evidence that the average temperature on earth has been getting warmer over the past few decades as a result of human activity?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_climate_change_nonbeliever %}
Haystaq modeled score (0-100) - Climate Change Nonbeliever; survey question: "From what youve read and heard, is there solid evidence that the average temperature on earth has been getting warmer over the past few decades as a result of human activity?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_gas_tax_oppose %}
Haystaq modeled score (0-100) - Gas Tax Oppose; survey question: "In general, would you support or oppose increased taxes on gas to fund road repairs?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_gas_tax_support %}
Haystaq modeled score (0-100) - Gas Tax Support; survey question: "In general, would you support or oppose increased taxes on gas to fund road repairs?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_green_new_deal_oppose %}
Haystaq modeled score (0-100) - Green New Deal Oppose; survey question: "From what you know now do you support or oppose the Green New Deal?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_green_new_deal_support %}
Haystaq modeled score (0-100) - Green New Deal Support; survey question: "From what you know now do you support or oppose the Green New Deal?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_pipeline_fracking_oppose %}
Haystaq modeled score (0-100) - Pipeline Fracking Oppose; survey question: "Are you likely to support or oppose new fossil fuel infrastructure, including fracking wells and oil and natural gas pipelines?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_pipeline_fracking_support %}
Haystaq modeled score (0-100) - Pipeline Fracking Support; survey question: "Are you likely to support or oppose new fossil fuel infrastructure, including fracking wells and oil and natural gas pipelines?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_abortion_pro_choice %}
Haystaq modeled score (0-100) - Abortion Pro Choice; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_abortion_pro_life %}
Haystaq modeled score (0-100) - Abortion Pro Life; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_general_anti_vax_pro_vax %}
Haystaq modeled score (0-100) - General Anti Vax Pro Vax; survey question: "Separate from Covid-19, how do you feel about mandating childhood vaccinations such as Measles, Mumps and Rubella for all children who have fully functional immune systems? Do you believe: 1. childhood vaccinations should be required, 2. vaccinations should be heavily encouraged, but not required, 3. families should make their own choices, or 4. vaccinations should be discouraged?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_medicaid_expansion_oppose %}
Haystaq modeled score (0-100) - Medicaid Expansion Oppose; survey question: "Some states have expanded Medicaid coverage to low-income residents while other states have not. Those in favor contend that federal funding, which comes from taxpayers in all 50 states, covers the majority of expenses for a program giving healthcare access to many who would otherwise be uninsured. Those opposed argue that the program could lead to unexpected costs for the state. Are you in favor of Medicaid Expansion in your own state?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_medicaid_expansion_support %}
Haystaq modeled score (0-100) - Medicaid Expansion Support; survey question: "Some states have expanded Medicaid coverage to low-income residents while other states have not. Those in favor contend that federal funding, which comes from taxpayers in all 50 states, covers the majority of expenses for a program giving healthcare access to many who would otherwise be uninsured. Those opposed argue that the program could lead to unexpected costs for the state. Are you in favor of Medicaid Expansion in your own state?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_medicare_for_all_oppose %}
Haystaq modeled score (0-100) - Medicare For All Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_medicare_for_all_support %}
Haystaq modeled score (0-100) - Medicare For All Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_obamacare_aca_expand %}
Haystaq modeled score (0-100) - Obamacare Aca Expand; survey question: "Which comes closest to your opinion on the Affordable Care Act or Obamacare: that it is beneficial but doesn’t go far enough, that it is about right, or that it goes too far and should be repealed or reformed?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_obamacare_aca_oppose %}
Haystaq modeled score (0-100) - Obamacare Aca Oppose; survey question: "Which comes closest to your opinion on the Affordable Care Act or Obamacare: that it is beneficial but doesn’t go far enough, that it is about right, or that it goes too far and should be repealed or reformed?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_obamacare_aca_protect %}
Haystaq modeled score (0-100) - Obamacare Aca Protect; survey question: "Which comes closest to your opinion on the Affordable Care Act or Obamacare: that it is beneficial but doesn’t go far enough, that it is about right, or that it goes too far and should be repealed or reformed?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_area_identity_rural %}
Haystaq issue score (0-100): area identity rural
{% enddocs %}

{% docs sav_hs_area_identity_suburban %}
Haystaq issue score (0-100): area identity suburban
{% enddocs %}

{% docs sav_hs_area_identity_urban %}
Haystaq issue score (0-100): area identity urban
{% enddocs %}

{% docs sav_hs_gamer_no %}
Haystaq issue score (0-100): gamer no
{% enddocs %}

{% docs sav_hs_gamer_yes %}
Haystaq issue score (0-100): gamer yes
{% enddocs %}

{% docs sav_hs_listen_podcaster_carlson %}
Haystaq issue score (0-100): listen podcaster carlson
{% enddocs %}

{% docs sav_hs_listen_podcaster_daily %}
Haystaq issue score (0-100): listen podcaster daily
{% enddocs %}

{% docs sav_hs_listen_podcaster_meidas %}
Haystaq issue score (0-100): listen podcaster meidas
{% enddocs %}

{% docs sav_hs_listen_podcaster_rogan %}
Haystaq issue score (0-100): listen podcaster rogan
{% enddocs %}

{% docs sav_hs_listen_podcaster_left_leaning %}
Haystaq issue score (0-100): listen podcaster left leaning
{% enddocs %}

{% docs sav_hs_listen_podcaster_right_leaning %}
Haystaq issue score (0-100): listen podcaster right leaning
{% enddocs %}

{% docs sav_hs_news_independent %}
Haystaq issue score (0-100): news independent
{% enddocs %}

{% docs sav_hs_news_mainstream %}
Haystaq issue score (0-100): news mainstream
{% enddocs %}

{% docs sav_hs_podcast_listener_no %}
Haystaq modeled score (0-100) - Podcast Listener No; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_podcast_listener_yes %}
Haystaq modeled score (0-100) - Podcast Listener Yes; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_social_media_user_frequent %}
Haystaq issue score (0-100): social media user frequent
{% enddocs %}

{% docs sav_hs_social_media_user_no_or_infrequent %}
Haystaq modeled score (0-100) - Social Media User No Or Infrequent; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_tv_news_source_most_trusted_cnn %}
Haystaq issue score (0-100): tv news source most trusted cnn
{% enddocs %}

{% docs sav_hs_tv_news_source_most_trusted_fox %}
Haystaq issue score (0-100): tv news source most trusted fox
{% enddocs %}

{% docs sav_hs_tv_news_source_most_trusted_msnbc %}
Haystaq issue score (0-100): tv news source most trusted msnbc
{% enddocs %}

{% docs sav_hs_tv_news_source_most_trusted_newsmax %}
Haystaq issue score (0-100): tv news source most trusted newsmax
{% enddocs %}

{% docs sav_hs_china_foreign_policy_advesarial %}
Haystaq modeled score (0-100) - China Foreign Policy Advesarial; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_china_foreign_policy_work_with %}
Haystaq modeled score (0-100) - China Foreign Policy Work With; survey question: "What comes closest to your opinion on China? That China is an important trade partner and we should find ways to work with them. Or that China represents an economic and military threat to the United States requiring a hardline approach?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_defense_spending_increase %}
Haystaq modeled score (0-100) - Defense Spending Increase; survey question: "What do you think of the US Defense Department’s budget? Should the DoD budget be: greatly reduced, slightly reduced, left where it is, slightly increased, or greatly increased?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_defense_spending_reduce %}
Haystaq modeled score (0-100) - Defense Spending Reduce; survey question: "What do you think of the US Defense Department’s budget? Should the DoD budget be: greatly reduced, slightly reduced, left where it is, slightly increased, or greatly increased?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_illegal_imm_process_unfair %}
Haystaq issue score (0-100): illegal imm process unfair
{% enddocs %}

{% docs sav_hs_illegal_imm_undesirable %}
Haystaq modeled score (0-100) - Illegal Imm Undesirable; survey question: "When it comes to immigration, is your bigger concern that too many undesirable people are coming into our country or that we are being unfair or even inhumane to immigrants and their families?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_israel_committing_genocide_no %}
Haystaq issue score (0-100): israel committing genocide no
{% enddocs %}

{% docs sav_hs_israel_committing_genocide_yes %}
Haystaq issue score (0-100): israel committing genocide yes
{% enddocs %}

{% docs sav_hs_israel_military_actions_oppose %}
Haystaq modeled score (0-100) - Israel Military Actions Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_israel_military_actions_support %}
Haystaq modeled score (0-100) - Israel Military Actions Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_mass_deportations_oppose %}
Haystaq issue score (0-100): mass deportations oppose
{% enddocs %}

{% docs sav_hs_mass_deportations_support %}
Haystaq issue score (0-100): mass deportations support
{% enddocs %}

{% docs sav_hs_mexican_wall_oppose %}
Haystaq modeled score (0-100) - Mexican Wall Oppose; survey question: "Do you support building a wall between the US and Mexico?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_mexican_wall_support %}
Haystaq modeled score (0-100) - Mexican Wall Support; survey question: "Do you support building a wall between the US and Mexico?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_military_family_relationship_no %}
Haystaq modeled score (0-100) - Military Family Relationship No; survey question: "Have you or any immediate family members served in the US Military?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_military_family_relationship_yes %}
Haystaq modeled score (0-100) - Military Family Relationship Yes; survey question: "Have you or any immediate family members served in the US Military?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_military_family_self %}
Haystaq issue score (0-100): military family self
{% enddocs %}

{% docs sav_hs_super_power_policy_oppose %}
Haystaq issue score (0-100): super power policy oppose
{% enddocs %}

{% docs sav_hs_super_power_policy_support %}
Haystaq issue score (0-100): super power policy support
{% enddocs %}

{% docs sav_hs_artificial_intelligence_excited %}
Haystaq modeled score (0-100) - Artificial Intelligence Excited; survey question: "Does the increased use of artificial intelligence in daily life make you feel more excited or more concerned?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_christian_values_not_positive %}
Haystaq issue score (0-100): christian values not positive
{% enddocs %}

{% docs sav_hs_christian_values_positive %}
Haystaq issue score (0-100): christian values positive
{% enddocs %}

{% docs sav_hs_college_admissions_consider_race %}
Haystaq modeled score (0-100) - College Admissions Consider Race; survey question: "Recently, the U.S. Supreme Court decided colleges and universities can no longer take race or ethnicity into consideration during the admissions process. Should colleges and universities be allowed to consider race during admissions?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_college_admissions_do_not_consider_race %}
Haystaq modeled score (0-100) - College Admissions Do Not Consider Race; survey question: "Recently, the U.S. Supreme Court decided colleges and universities can no longer take race or ethnicity into consideration during the admissions process. Should colleges and universities be allowed to consider race during admissions?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_concerned_job_loss_due_to_ai_no %}
Haystaq issue score (0-100): concerned job loss due to ai no
{% enddocs %}

{% docs sav_hs_concerned_job_loss_due_to_ai_yes %}
Haystaq issue score (0-100): concerned job loss due to ai yes
{% enddocs %}

{% docs sav_hs_right_wing_conspiracy_believer %}
Haystaq issue score (0-100): right wing conspiracy believer
{% enddocs %}

{% docs sav_hs_right_wing_conspiracy_nonbeliever %}
Haystaq issue score (0-100): right wing conspiracy nonbeliever
{% enddocs %}

{% docs sav_hs_dei_oppose %}
Haystaq modeled score (0-100) - Dei Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_dei_support %}
Haystaq modeled score (0-100) - Dei Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_doge_oppose %}
Haystaq modeled score (0-100) - Doge Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_doge_support %}
Haystaq modeled score (0-100) - Doge Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_domestic_deployment_of_troops_oppose %}
Haystaq issue score (0-100): domestic deployment of troops oppose
{% enddocs %}

{% docs sav_hs_domestic_deployment_of_troops_support %}
Haystaq issue score (0-100): domestic deployment of troops support
{% enddocs %}

{% docs sav_hs_epstein_files_important %}
Haystaq issue score (0-100): epstein files important
{% enddocs %}

{% docs sav_hs_epstein_files_unimportant_or_hoax %}
Haystaq issue score (0-100): epstein files unimportant or hoax
{% enddocs %}

{% docs sav_hs_ice_actions_oppose %}
Haystaq issue score (0-100): ice actions oppose
{% enddocs %}

{% docs sav_hs_ice_actions_support %}
Haystaq issue score (0-100): ice actions support
{% enddocs %}

{% docs sav_hs_jan_6th_pardons_oppose %}
Haystaq modeled score (0-100) - Jan 6Th Pardons Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_jan_6th_pardons_support %}
Haystaq modeled score (0-100) - Jan 6Th Pardons Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_police_trust_no %}
Haystaq modeled score (0-100) - Police Trust No; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_police_trust_yes %}
Haystaq modeled score (0-100) - Police Trust Yes; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_political_troll_entertaining %}
Haystaq issue score (0-100): political troll entertaining
{% enddocs %}

{% docs sav_hs_political_troll_negative %}
Haystaq issue score (0-100): political troll negative
{% enddocs %}

{% docs sav_hs_same_sex_marriage_oppose %}
Haystaq modeled score (0-100) - Same Sex Marriage Oppose; survey question: "Do you support or oppose same-sex marriage?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_same_sex_marriage_support %}
Haystaq modeled score (0-100) - Same Sex Marriage Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_snap_not_important %}
Haystaq issue score (0-100): snap not important
{% enddocs %}

{% docs sav_hs_snap_protect %}
Haystaq issue score (0-100): snap protect
{% enddocs %}

{% docs sav_hs_traditional_gender_roles_negative %}
Haystaq issue score (0-100): traditional gender roles negative
{% enddocs %}

{% docs sav_hs_traditional_gender_roles_positive %}
Haystaq issue score (0-100): traditional gender roles positive
{% enddocs %}

{% docs sav_hs_trans_athlete_no %}
Haystaq modeled score (0-100) - Trans Athlete No; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_trans_athlete_yes %}
Haystaq modeled score (0-100) - Trans Athlete Yes; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_violent_crime_not_worried %}
Haystaq modeled score (0-100) - Violent Crime Not Worried; survey question: "How much do you worry about violent crime affecting you or your family? Are you very worried, somewhat worried, not very worried or not at all worried about violent crime affecting you or your family?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_violent_crime_worried %}
Haystaq issue score (0-100): violent crime worried
{% enddocs %}

{% docs sav_hs_voting_fraud_concern_barriers %}
Haystaq issue score (0-100): voting fraud concern barriers
{% enddocs %}

{% docs sav_hs_voting_fraud_concern_fraud %}
Haystaq modeled score (0-100) - Voting Fraud Concern Fraud; survey question: "Which is the greater concern for you around voting - that there are too many people fraudulently casting votes or that there are barriers that make it difficult for people to vote?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_campaign_finance_reform_high_priority %}
Haystaq issue score (0-100): campaign finance reform high priority
{% enddocs %}

{% docs sav_hs_campaign_finance_reform_less_important %}
Haystaq issue score (0-100): campaign finance reform less important
{% enddocs %}

{% docs sav_hs_casino_oppose %}
Haystaq modeled score (0-100) - Casino Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_casino_support %}
Haystaq modeled score (0-100) - Casino Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_death_penalty_oppose %}
Haystaq modeled score (0-100) - Death Penalty Oppose; survey question: "Do you favor or oppose the death penalty?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_death_penalty_support %}
Haystaq modeled score (0-100) - Death Penalty Support; survey question: "Do you favor or oppose the death penalty?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_gun_control_oppose %}
Haystaq modeled score (0-100) - Gun Control Oppose; survey question: "Do you believe that the laws covering the sale and ownership of guns should be made stronger, made weaker or kept as they are now?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_gun_control_support %}
Haystaq modeled score (0-100) - Gun Control Support; survey question: "Do you believe that the laws covering the sale and ownership of guns should be made stronger, made weaker or kept as they are now?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_marijuana_legal_oppose %}
Haystaq modeled score (0-100) - Marijuana Legal Oppose; survey question: "Thinking about marijuana laws in the US, do you support the full legalization of marijuana; or, do you support laws permitting the use of medical marijuana; or, do you oppose any laws permitting the use of marijuana?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_marijuana_legal_support %}
Haystaq modeled score (0-100) - Marijuana Legal Support; survey question: "Thinking about marijuana laws in the US, do you support the full legalization of marijuana; or, do you support laws permitting the use of medical marijuana; or, do you oppose any laws permitting the use of marijuana?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_online_gambling_less_legal %}
Haystaq modeled score (0-100) - Online Gambling Less Legal; survey question: "Do you believe that internet gaming like online poker, daily fantasy, and internet sports betting should be legal and regulated?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_online_gambling_more_legal %}
Haystaq modeled score (0-100) - Online Gambling More Legal; survey question: "Do you believe that internet gaming like online poker, daily fantasy, and internet sports betting should be legal and regulated?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_public_transit_oppose %}
Haystaq modeled score (0-100) - Public Transit Oppose; survey question: "Would you support expanded funding for public transportation such as buses, subways and commuter rail in your state or community even if it meant your taxes could go up?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_public_transit_support %}
Haystaq modeled score (0-100) - Public Transit Support; survey question: "Would you support expanded funding for public transportation such as buses, subways and commuter rail in your state or community even if it meant your taxes could go up?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_regulations_good %}
Haystaq modeled score (0-100) - Regulations Good; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_regulations_too_harsh %}
Haystaq modeled score (0-100) - Regulations Too Harsh; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_stadium_public_financing_approve %}
Haystaq modeled score (0-100) - Stadium Public Financing Approve; survey question: "Generally, would you approve of the use of your local taxes to fund the building or enhancement of a professional sports stadium to attract or retain a team?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_stadium_public_financing_disapprove %}
Haystaq modeled score (0-100) - Stadium Public Financing Disapprove; survey question: "Generally, would you approve of the use of your local taxes to fund the building or enhancement of a professional sports stadium to attract or retain a team?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_likely_ev %}
Haystaq modeled score (0-100) - Likely Ev; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_likely_mid_term_voter %}
Haystaq modeled score (0-100) - Likely Mid Term Voter; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_likely_presidential_voter %}
Haystaq modeled score (0-100) - Likely Presidential Voter; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_likely_vbm %}
Haystaq modeled score (0-100) - Likely Vbm; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_felon_voting_oppose %}
Haystaq modeled score (0-100) - Felon Voting Oppose; survey question: "Right now, some people who have been convicted of a felony and have served their sentence are not allowed to vote. Do you think that a felon who has paid their debt to society should be allowed the opportunity to vote, or do you think felons forfeited their right to vote?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_felon_voting_support %}
Haystaq modeled score (0-100) - Felon Voting Support; survey question: "Right now, some people who have been convicted of a felony and have served their sentence are not allowed to vote. Do you think that a felon who has paid their debt to society should be allowed the opportunity to vote, or do you think felons forfeited their right to vote?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_rank_choice_voting_oppose %}
Haystaq modeled score (0-100) - Rank Choice Voting Oppose; survey question: "Do you support electoral systems that allow voters to rank candidates in order of preference, widely known as Ranked Choice Voting?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_rank_choice_voting_support %}
Haystaq modeled score (0-100) - Rank Choice Voting Support; survey question: "Do you support electoral systems that allow voters to rank candidates in order of preference, widely known as Ranked Choice Voting?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_redistricting_indep_com %}
Haystaq modeled score (0-100) - Redistricting Indep Com; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_redistricting_state_leg %}
Haystaq modeled score (0-100) - Redistricting State Leg; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_conspiracy_nonbeliever %}
Haystaq modeled score (0-100) - Conspiracy Nonbeliever; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_conspiracy_believer %}
Haystaq modeled score (0-100) - Conspiracy Believer; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_likely_polling_turnout %}
Haystaq modeled score (0-100) - Likely Polling Turnout; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_wealth_acquired_hardwork %}
Haystaq modeled score (0-100) - Wealth Acquired Hardwork; survey question: "Do you think that most people in the top 1% acquired their wealth because they worked hard or because of advantages they were handed?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_wealth_acquired_advantages %}
Haystaq modeled score (0-100) - Wealth Acquired Advantages; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_voting_fraud_concern_oppression %}
Haystaq modeled score (0-100) - Voting Fraud Concern Oppression; survey question: "Which is the greater concern for you around voting - that there are too many people fraudulently casting votes or that there are barriers that make it difficult for people to vote?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_violent_crime_very_worried %}
Haystaq modeled score (0-100) - Violent Crime Very Worried; survey question: "How much do you worry about violent crime affecting you or your family? Are you very worried, somewhat worried, not very worried or not at all worried about violent crime affecting you or your family?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_united_healthcare_no_fault %}
Haystaq modeled score (0-100) - United Healthcare No Fault; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_united_healthcare_at_fault %}
Haystaq modeled score (0-100) - United Healthcare At Fault; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_tv_most_trusted_news_msnbc %}
Haystaq modeled score (0-100) - Tv Most Trusted News Msnbc; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_tv_most_trusted_news_fox %}
Haystaq modeled score (0-100) - Tv Most Trusted News Fox; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_tv_most_trusted_news_cnn %}
Haystaq modeled score (0-100) - Tv Most Trusted News Cnn; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_trust_science_always %}
Haystaq modeled score (0-100) - Trust Science Always; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_trust_science_rarely %}
Haystaq modeled score (0-100) - Trust Science Rarely; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_tribalism_open_minded %}
Haystaq modeled score (0-100) - Tribalism Open Minded; survey question: "When it comes to attempts to modernize the US Military, who do you think has or had a better policy, Trump or Biden? And When it comes to allowing states to import cheaper prescription drugs from Canada, who do you think has or had a better policy, Trump or Biden?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_social_security_tax_increase_support %}
Haystaq modeled score (0-100) - Social Security Tax Increase Support; survey question: "To preserve Social Security, would you support a small payroll tax increase and/or raising the cap that exempts income above $118,000?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_social_security_tax_increase_oppose %}
Haystaq modeled score (0-100) - Social Security Tax Increase Oppose; survey question: "To preserve Social Security, would you support a small payroll tax increase and/or raising the cap that exempts income above $118,000?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_social_media_user %}
Haystaq modeled score (0-100) - Social Media User; survey question: "How often do you use social media such as Facebook, Twitter, Snapchat or Instagram?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_social_media_truth_vs_speech_truth %}
Haystaq modeled score (0-100) - Social Media Truth Vs Speech Truth; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_social_media_truth_vs_speech_free_speech %}
Haystaq modeled score (0-100) - Social Media Truth Vs Speech Free Speech; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_sell_federal_lands_support %}
Haystaq modeled score (0-100) - Sell Federal Lands Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_sell_federal_lands_oppose %}
Haystaq modeled score (0-100) - Sell Federal Lands Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_opioid_crisis_treat %}
Haystaq modeled score (0-100) - Opioid Crisis Treat; survey question: "What do you think the best strategy is for dealing with the opioid crisis, tougher enforcement of our laws for drug dealers and users or increased treatment programs?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_opioid_crisis_enforce %}
Haystaq modeled score (0-100) - Opioid Crisis Enforce; survey question: "What do you think the best strategy is for dealing with the opioid crisis, tougher enforcement of our laws for drug dealers and users or increased treatment programs?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_min_wage_15_increase_support %}
Haystaq modeled score (0-100) - Min Wage 15 Increase Support; survey question: "Do you support proposals to make the national minimum wage $15 per hour?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_min_wage_15_increase_oppose %}
Haystaq modeled score (0-100) - Min Wage 15 Increase Oppose; survey question: "Do you support proposals to make the national minimum wage $15 per hour?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_mass_deporations_oppose %}
Haystaq modeled score (0-100) - Mass Deporations Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_mass_deporations_support %}
Haystaq modeled score (0-100) - Mass Deporations Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_jobs_guarantee_support %}
Haystaq modeled score (0-100) - Jobs Guarantee Support; survey question: "Do you support or oppose the idea of a Federal Jobs Guarantee, which would create a publically funded job for any American who wants one. Support A Jobs Guarantee, Oppose A Jobs Guarantee, Unsure"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_jobs_guarantee_oppose %}
Haystaq modeled score (0-100) - Jobs Guarantee Oppose; survey question: "Do you support or oppose the idea of a Federal Jobs Guarantee, which would create a publically funded job for any American who wants one. Support A Jobs Guarantee, Oppose A Jobs Guarantee, Unsure"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_insurance_of_last_resort_government_should_not_provide %}
Haystaq modeled score (0-100) - Insurance Of Last Resort Government Should Not Provide; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_insurance_of_last_resort_government_should_provide %}
Haystaq modeled score (0-100) - Insurance Of Last Resort Government Should Provide; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_immigration_undesirable %}
Haystaq modeled score (0-100) - Immigration Undesirable; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_immigration_process_unfair %}
Haystaq modeled score (0-100) - Immigration Process Unfair; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_family_medical_leave_support %}
Haystaq modeled score (0-100) - Family Medical Leave Support; survey question: "Do you support or oppose paid family and medical leave for all US workers?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_family_medical_leave_oppose %}
Haystaq modeled score (0-100) - Family Medical Leave Oppose; survey question: "Do you support or oppose paid family and medical leave for all US workers?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_state_level_fema_support %}
Haystaq modeled score (0-100) - State Level Fema Support; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_state_level_fema_oppose %}
Haystaq modeled score (0-100) - State Level Fema Oppose; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_despondency_on_pace %}
Haystaq modeled score (0-100) - Despondency On Pace; survey question: "How close are you to where you thought you would be at this stage of your life?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_despondency_far_behind %}
Haystaq modeled score (0-100) - Despondency Far Behind; survey question: "How close are you to where you thought you would be at this stage of your life?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_despondency_ahead %}
Haystaq modeled score (0-100) - Despondency Ahead; survey question: "How close are you to where you thought you would be at this stage of your life?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_community_college_free_support %}
Haystaq modeled score (0-100) - Community College Free Support; survey question: "Should Community College be free of charge to enrolled students?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_community_college_free_oppose %}
Haystaq modeled score (0-100) - Community College Free Oppose; survey question: "Should Community College be free of charge to enrolled students?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_civil_liberties_support %}
Haystaq modeled score (0-100) - Civil Liberties Support; survey question: "What concerns you more about the governments anti-terrorism policies – that they have gone too far and are restricting the average persons civil liberties, or that they have not gone far enough and are not adequately protecting us?"; higher score = more likely to hold the stated/positive position.
{% enddocs %}

{% docs sav_hs_civil_liberties_oppose %}
Haystaq modeled score (0-100) - Civil Liberties Oppose; survey question: "What concerns you more about the governments anti-terrorism policies – that they have gone too far and are restricting the average persons civil liberties, or that they have not gone far enough and are not adequately protecting us?"; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_candidate_mail_read_carefully %}
Haystaq modeled score (0-100) - Candidate Mail Read Carefully; survey question: "Do you read mail sent by candidates running for office?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_candidate_mail_do_not_read %}
Haystaq modeled score (0-100) - Candidate Mail Do Not Read; survey question: "Do you read mail sent by candidates running for office?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_autonomous_vehicles_do_not_allow %}
Haystaq modeled score (0-100) - Autonomous Vehicles Do Not Allow; survey question: "In your opinion, should self-driving or autonomous vehicles be allowed on the road?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_autonomous_vehicles_allow %}
Haystaq modeled score (0-100) - Autonomous Vehicles Allow; survey question: "In your opinion, should self-driving or autonomous vehicles be allowed on the road?"; higher score = more aligned with the modeled position.
{% enddocs %}

{% docs sav_hs_aliens_governenment_hiding_much %}
Haystaq modeled score (0-100) - Aliens Governenment Hiding Much; higher score = more likely to hold the opposing/negative position.
{% enddocs %}

{% docs sav_hs_aliens_governenment_disclosed_all %}
Haystaq modeled score (0-100) - Aliens Governenment Disclosed All; higher score = more likely to hold the stated/positive position.
{% enddocs %}
