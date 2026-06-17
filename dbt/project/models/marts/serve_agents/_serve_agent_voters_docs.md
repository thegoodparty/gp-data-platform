{# Column documentation for serve_agent_voters.
   Auto-generated from seeds/serve_agent_voters_columns.csv, whose
   descriptions are sourced from L2's own data dictionaries
   (per-state uniform data dictionary; sandbox.haystaq_data_dictionary). #}

{% docs sav_voter_key %}
Stable pseudonymous voter key: SHA-256 hash of L2's LALVOTERID. Uniquely identifies a constituent for counting/joining without exposing the underlying L2 identifier. One row per voter.
{% enddocs %}

{% docs sav_Residence_Addresses_City %}
Residence Addresses City
{% enddocs %}

{% docs sav_Residence_Addresses_Zip %}
Residence Addresses Zip
{% enddocs %}

{% docs sav_Residence_Addresses_CensusTract %}
6 digit census tract number
{% enddocs %}

{% docs sav_Residence_Addresses_CensusBlockGroup %}
Single digit number
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
County
{% enddocs %}

{% docs sav_Voters_FIPS %}
County FIPS Code
{% enddocs %}

{% docs sav_County_Commissioner_District %}
County Commissioner District
{% enddocs %}

{% docs sav_County_Supervisorial_District %}
County Supervisorial District
{% enddocs %}

{% docs sav_Precinct %}
Precinct
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
The estimated number of adults per square mile calculated for all adults living within the census block.
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
Combination Flags: High Turnout and Likely Early Voters (flag=1 means condition met)
{% enddocs %}

{% docs sav_hf_likely_presidential_voter_and_likely_vbm %}
Haystaq issue flag: likely presidential voter and likely vbm
{% enddocs %}

{% docs sav_hf_likely_presidential_voter_and_unlikely_ev %}
Combination Flags: Low Turnout and Unlikely Early Voters (flag=1 means condition met)
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
Political Activism: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_attends_church_frequently %}
Church Attendance: Respondents who said almost every week,at least once a week vs. those who said rarely,never (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_attends_church_never %}
Church Attendance: Respondents who said never vs. those who said at least once a week, rarely, almost every week, about once a month (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_candidate_mail_readership_do_not_read %}
Haystaq issue score (0-100): candidate mail readership do not read
{% enddocs %}

{% docs sav_hs_candidate_mail_readership_read_carefully %}
Haystaq issue score (0-100): candidate mail readership read carefully
{% enddocs %}

{% docs sav_hs_charity_giving_enviro_cause %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_charity_giving_international_aid %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_charity_giving_performing_arts %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_charity_giving_relig_cause %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_charity_giving_vet_cause %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
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
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_dropoff_fill_only_top %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_most_important_policy_item_economics %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_most_important_policy_item_environment %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_most_important_policy_item_help_people %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_most_important_policy_item_religious_values %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_most_important_policy_keep_safe %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_political_donations_likely %}
Political Donations: Respondents who said very likely vs. those who said very unlikely (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_political_donations_unlikely %}
Political Donations: This DV is the inverse of the support score above (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_religion_important %}
Importance of Religion: Respondents who said ‘extremely important vs. those who said not important at all (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_religion_not_important %}
Importance of Religion: Respondents who said not important at all vs. those who said extremely important, somewhat important, moderately important (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_responsiveness_email %}
Responsiveness: Respondents via email to the survet vs. those who we attempted to reach but who did not respond to the survey via email (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_responsiveness_live %}
Responsiveness: Respondents via live calling to the survey vs. those who we attempted to reach but who did not respond to the survey via live calling (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_responsiveness_sms %}
Responsiveness: Respondents via texting to the surve vs. those who we attempted to reach but who did not respond to the survey via texting (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_age_limit_oppose %}
Age Limit (elected officials): field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_age_limit_support %}
Age Limit (elected officials): field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_candidate_female_do_not_prefer %}
Female Candidates: Inverse of the score above (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_candidate_female_prefer %}
Female Candidates: Respondents who said prefer vs. those who said do not favor, do not consider (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_candidate_minority_do_not_prefer %}
Minority Candidates: Respondents who said prefer vs. those who said do not favor inversed (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_candidate_minority_prefer %}
Minority Candidates: Respondents who said prefer vs. those who said do not favor, ‘Do Not Consider Race’ (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_consumer_value_brand_savings %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_consumer_value_environment %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_consumer_value_low_cost %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_electric_vehicle_likely_buyer %}
Electric Vehicle Ownership: Respondents who answered ‘already own’, ‘extremely likely’ vs those who answered ‘unlikely’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_electric_vehicle_not_likely %}
Electric Vehicle Ownership: Respondents who answered ‘unlikely’ vs those who answered all other responses (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_any_home_buyer %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_new_home_buyer %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_rideshare_user %}
Rideshare User: Respondents who said ‘yes’ vs. those who said ‘no’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_rideshare_user_no %}
Rideshare User: Inverse of Consumer_Focus_Rideshare_Use_Rideshare_User (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_solar_panel_buyer_no %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_solar_panel_buyer_yes %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_tv_viewer_free_streaming %}
TV Viewing: Respondents who said free platforms vs. those who said traditional tv,paid streaming,do not watch (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_tv_viewer_not_viewer %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_tv_viewer_watch_any_tv %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_tv_viewer_watch_paid_streaming %}
TV Viewing: Respondents who said paid streaming’ vs. those who said traditional tv,free platforms,do not watch (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_vaping_user_no %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_vaping_user_yes %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_affordability_changed_what_you_buy_no %}
Haystaq issue score (0-100): affordability changed what you buy no
{% enddocs %}

{% docs sav_hs_affordability_changed_what_you_buy_yes %}
Haystaq issue score (0-100): affordability changed what you buy yes
{% enddocs %}

{% docs sav_hs_affordable_housing_gov_has_role %}
Affordable Housing: Respondents who said government role vs those who said market forces (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_affordable_housing_gov_no_role %}
Affordable Housing: Respondents who said market forces vs those who said government role,undecided (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_amazon_exploitative %}
Amazon: Respondents who said exploitive vs. those who said good jobs,unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_amazon_good_jobs %}
Amazon: Respondents who said good jobs vs. those who said exploitive (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_unions_beneficial %}
Unions: Respondents who said mostly beneficial’ vs. those who said mostly harmful (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_unions_not_beneficial %}
Unions: Respondents who said mostly harmful vs. those who said mostly beneficial’,’unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_capitalism_believe_flawed %}
View On Capitalism: Respondents who said fundamentally flawed,major flaws vs. those who said fundamentally sound or minor flaws (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_capitalism_believe_sound %}
View On Capitalism: Respondents who said fundamentally sound,minor flaws vs. those who said fundamentally flawed, major flaws, or unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_crypto_buyer_no %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_crypto_buyer_yes %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_crypto_increase_restrictions %}
Crypto Currency: Respondents who said protect more,severe restrictions vs. those who said no role,unsure,current rules ok (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_crypto_reduce_leave_as_is %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_crypto_reduce_restrictions %}
Crypto Currency: Respondents who said no role vs. those who said protect more, severe restrictions, current rules ok, or unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_econ_anxiety_not_worried %}
Economic Anxiety: Respondents who said (not worried) vs. those who said (very worried, somewhat worried, unsure) (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_econ_anxiety_very_worried %}
Economic Anxiety: Respondents who said (very worried) vs. those who said (not worried) (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_economic_despondency_ahead %}
Haystaq issue score (0-100): economic despondency ahead
{% enddocs %}

{% docs sav_hs_economic_despondency_far_behind %}
Haystaq issue score (0-100): economic despondency far behind
{% enddocs %}

{% docs sav_hs_gentrification_oppose %}
Gentrification: Respondents who said mostly harmful vs. those who said mostly positive or unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_gentrification_support %}
Gentrification: Respondents who said mostly positive vs. those who said mostly harmful or unsure (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_gig_work_keep_contractor %}
Gig Opinion: Respondents who said independent contractors vs. those who said employees (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_gig_work_make_employees %}
Gig Opinion: Respondents who said employees vs. those who said independent contractors,’unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_gig_worker_ever %}
Gig Worker: Respondents who said part-time,full-time,did in the past,consider vs. those who said never (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_gig_worker_now %}
Gig Worker: Respondents who said ‘part-time,full-time vs. those who said never (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_gig_worker_unlikely %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_income_inequality_no_issue %}
Income Inequality: Respondents who said not much of a problem, not a problem vs. those who said big problem,somewhat of a problem,unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_income_inequality_serious %}
Income Inequality: Respondents who said big problem vs. those who said somewhat of a problem,not much of a problem, not a problem,unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_inflation_fault_biden %}
Inflation Fault: Respondents who said biden administration vs. those who said corporate america, external events, higher wages, or unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_inflation_fault_corporate_america %}
Inflation Fault: Respondents who said corporate america vs. those who said biden administration, external events, higher wages, or unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_inflation_fault_external_events %}
Inflation Fault: Respondents who said external events vs. those who said biden administration, corporate america, higher wages, or unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_infrastructure_funding_enough_spent %}
Infrastructure Spending (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_infrastructure_funding_fund_more %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_job_seeker_does_not_work %}
Job Seeker: Respondents who answered ‘I am Retired or Do Not Work’ vs those who answered ‘Unlikely’, ‘Likely’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_job_seeker_likely %}
Job Seeker: Respondents who answered ‘Likely’ vs those who answered ‘I am Retired or Do Not Work’, ‘Unlikely’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_job_seeker_unlikely %}
Job Seeker: Respondents who answered ‘Unlikely’ vs those who answered ‘I am Retired or Do Not Work’, ‘Likely’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_tax_cuts_oppose %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_tax_cuts_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_charter_schools_oppose %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_charter_schools_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_critical_race_theory_books_ban %}
Critical Race Theory: Respondents who said ban all vs. those who said ban none (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_critical_race_theory_books_do_not_ban %}
Critical Race Theory: Respondents who said ban none vs. those who said ban all, ban some, or unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_school_choice_oppose %}
School Choice: This DV is the inverse of the school choice support score below (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_school_choice_support %}
School Choice: Respondents who said school choice vs. those who said no school choice (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_school_funding_less %}
School Spending: This DV is the inverse of the support school funding score above (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_school_funding_more %}
School Spending: Respondents who said more funding for schools vs. those who said less funding for schools (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_teachers_union_negative %}
Teachers Unions: Respondents who said ‘negative’ vs. those who said ‘positive’ , ‘neutral’,’unsure’ (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_teachers_union_positive %}
Teachers Unions: Respondents who said ‘positive’ vs. those who said ‘negative’ (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_climate_change_believer %}
Environment- Climate Change: Respondents who said yes vs. those who said no, ‘unsure’ (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_climate_change_nonbeliever %}
Environment- Climate Change: Respondents who said no vs. those who said yes, ‘unsure’ (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_gas_tax_oppose %}
Gas Tax: Respondents who said strongly opposed, somewhat opposed vs. those who said strongly support, somewhat support,neutral/unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_gas_tax_support %}
Gas Tax: Respondents who said strongly support,somewhat support vs. those who said strongly opposed, somewhat opposed (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_green_new_deal_oppose %}
Green New Deal: Respondents who said strongly oppose, somewhat oppose vs. those who said strongly support, somewhat support, unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_green_new_deal_support %}
Green New Deal: Respondents who said strongly support, somewhat support vs. those who said strongly oppose, somewhat oppose or unsure (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_pipeline_fracking_oppose %}
oil Gas or Oil Pipelines: This DV is the inverse of the support fossil fuel infrastructure score (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_pipeline_fracking_support %}
oil Gas or Oil Pipelines: Respondents who said support new infrastructure vs. those who said oppose new infrastructure (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_abortion_pro_choice %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_abortion_pro_life %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_general_anti_vax_pro_vax %}
General Vaccines: Respondents who answered ‘required’ vs those who answered family choice,discouraged,unsure,encouraged (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_medicaid_expansion_oppose %}
Medicaid Expansion: Respondents who said against vs. those who said, ‘unsure’, for expansion (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_medicaid_expansion_support %}
Medicaid Expansion: Respondents who said for expansion vs. those who said against, ‘unsure’ (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_medicare_for_all_oppose %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_medicare_for_all_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_obamacare_aca_expand %}
Affordable Care Act/Obamacare: Respondents who said not far enough vs. those who said repeal,unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_obamacare_aca_oppose %}
Affordable Care Act/Obamacare: Respondents who said repeal vs. those who said ‘not far enough,good as is,unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_obamacare_aca_protect %}
Affordable Care Act/Obamacare: Respondents who said ‘good as is’ vs. those who said repeal,unsure (higher score = more aligned with dependent variable position)
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
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_podcast_listener_yes %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_social_media_user_frequent %}
Haystaq issue score (0-100): social media user frequent
{% enddocs %}

{% docs sav_hs_social_media_user_no_or_infrequent %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
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
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_china_foreign_policy_work_with %}
China Foreign Policy: Respondents who answered ‘Work With China’ vs those who answered ‘Hardline Approach’, ‘Unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_defense_spending_increase %}
Defense Spending: Respondents who said greatly increased vs. those who said greatly reduced,slightly reduced,no opinion, ok (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_defense_spending_reduce %}
Defense Spending: Respondents who said greatly reduced vs. those who said greatly increased (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_illegal_imm_process_unfair %}
Haystaq issue score (0-100): illegal imm process unfair
{% enddocs %}

{% docs sav_hs_illegal_imm_undesirable %}
View of Immigration: Respondents who said undesirable people vs. those who said ‘unfair to immigrants,’unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_israel_committing_genocide_no %}
Haystaq issue score (0-100): israel committing genocide no
{% enddocs %}

{% docs sav_hs_israel_committing_genocide_yes %}
Haystaq issue score (0-100): israel committing genocide yes
{% enddocs %}

{% docs sav_hs_israel_military_actions_oppose %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_israel_military_actions_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_mass_deportations_oppose %}
Haystaq issue score (0-100): mass deportations oppose
{% enddocs %}

{% docs sav_hs_mass_deportations_support %}
Haystaq issue score (0-100): mass deportations support
{% enddocs %}

{% docs sav_hs_mexican_wall_oppose %}
Mexican Wall: Respondents who said strongly oppose vs. those who said neutral or unsure, strongly support, somewhat support (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_mexican_wall_support %}
Mexican Wall: Respondents who said strongly support vs. those who said strongly oppose (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_military_family_relationship_no %}
Military Family Relationship: This DV is the inverse of the Yes- military family relationship score above (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_military_family_relationship_yes %}
Military Family Relationship: Respondents who said self, immediate family vs. those who said none (higher score = more aligned with dependent variable position)
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
AI: Respondents who answered excited vs those who answered unsure,equal,concerned (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_christian_values_not_positive %}
Haystaq issue score (0-100): christian values not positive
{% enddocs %}

{% docs sav_hs_christian_values_positive %}
Haystaq issue score (0-100): christian values positive
{% enddocs %}

{% docs sav_hs_college_admissions_consider_race %}
Race College Admissions: Respondents who answered consider race vs those who answered do not consider race,unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_college_admissions_do_not_consider_race %}
Race College Admissions: Respondents who answered do not consider race vs those who answered consider race,unsure (higher score = more aligned with dependent variable position)
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
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_dei_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_doge_oppose %}
DOGE (Dept of Gov Efficiency): field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_doge_support %}
DOGE (Dept of Gov Efficiency): field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
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
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_jan_6th_pardons_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_police_trust_no %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_police_trust_yes %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_political_troll_entertaining %}
Haystaq issue score (0-100): political troll entertaining
{% enddocs %}

{% docs sav_hs_political_troll_negative %}
Haystaq issue score (0-100): political troll negative
{% enddocs %}

{% docs sav_hs_same_sex_marriage_oppose %}
Same Sex Marriage (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_same_sex_marriage_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
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
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_trans_athlete_yes %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_violent_crime_not_worried %}
Violent Crime Anxiety: Respondents who said not at all worried vs. those who said, very worried,’somewhat worried’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_violent_crime_worried %}
Haystaq issue score (0-100): violent crime worried
{% enddocs %}

{% docs sav_hs_voting_fraud_concern_barriers %}
Haystaq issue score (0-100): voting fraud concern barriers
{% enddocs %}

{% docs sav_hs_voting_fraud_concern_fraud %}
Voting Fraud: Respondents who said voter fraud vs. voter barriers,’unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_campaign_finance_reform_high_priority %}
Haystaq issue score (0-100): campaign finance reform high priority
{% enddocs %}

{% docs sav_hs_campaign_finance_reform_less_important %}
Haystaq issue score (0-100): campaign finance reform less important
{% enddocs %}

{% docs sav_hs_casino_oppose %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_casino_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_death_penalty_oppose %}
Death Penalty: Respondents who said strongly oppose vs. those who said strongly favor,somewhat favor, or unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_death_penalty_support %}
Death Penalty: Respondents who said strongly favor vs. those who said strongly oppose, somewhat oppose, or unsure (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_gun_control_oppose %}
Gun Control: Respondents who said stronger vs. those who said weaker, kept as they are (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_gun_control_support %}
Gun Control: Respondents who said stronger vs. those who said weaker, kept as they are (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_marijuana_legal_oppose %}
Marijuana Legalization: Respondents who said oppose marijuana vs. those who said medical marijuana or full legalization of marijuana (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_marijuana_legal_support %}
Marijuana Legalization: Respondents who said full legalization of marijuana vs. those who said oppose marijuana or medical marijuana (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_online_gambling_less_legal %}
Online Gaming: Respondents who said remain largely illegal,more enforcement vs. those who said fully legalized,some restrictions eased,unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_online_gambling_more_legal %}
Online Gaming: Respondents who said fully legalized vs. those who said remain largely illegal,remain illegal and more enforcement (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_public_transit_oppose %}
Public Transit Funding: This DV is the inverse of the support transit funding score above (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_public_transit_support %}
Public Transit Funding: Respondents who said support funding vs. those who said oppose funding (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_regulations_good %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_regulations_too_harsh %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_stadium_public_financing_approve %}
Stadium Financing: Respondents who answered ‘strongly approve’ or ‘somewhat approve’ vs those who answered ‘somewhat disapprove’ or ‘strongly disapprove’ or ‘unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_stadium_public_financing_disapprove %}
Stadium Financing: Respondents who answered ‘strongly disapprove’ or ‘somewhat disapprove’ vs those who answered ‘strongly approve’ or ‘somewhat approve’ or ‘unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_likely_ev %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_likely_mid_term_voter %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_likely_presidential_voter %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_likely_vbm %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_felon_voting_oppose %}
Felons Voting: Respondents who said not allowed vs. those who said allowed, unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_felon_voting_support %}
Felons Voting: Respondents who said allowed vs. those who said not allowed, unsure (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_rank_choice_voting_oppose %}
Rank Choice Voting: Respondents who said strongly oppose,somewhat oppose vs. those who said strongly support,somewhat support,unfamiliar (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_rank_choice_voting_support %}
Rank Choice Voting: Respondents who said strongly support vs. those who said strongly oppose,somewhat oppose, unfamiliar (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_redistricting_indep_com %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_redistricting_state_leg %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_conspiracy_nonbeliever %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_conspiracy_believer %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_likely_polling_turnout %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_wealth_acquired_hardwork %}
Wealth Acquisition: Respondents who said hard work vs. those who said advantages (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_wealth_acquired_advantages %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_voting_fraud_concern_oppression %}
Voting Fraud: Respondents who said voter barriers vs.voter fraud,’unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_violent_crime_very_worried %}
Violent Crime Anxiety: Respondents who said very worried,’somewhat worried’ vs. those who said not at all worried, not very worried (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_united_healthcare_no_fault %}
United Healthcare: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_united_healthcare_at_fault %}
United Healthcare: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_tv_most_trusted_news_msnbc %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_tv_most_trusted_news_fox %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_tv_most_trusted_news_cnn %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_trust_science_always %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_trust_science_rarely %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_tribalism_open_minded %}
Tribalism: Respondents who said same policy,unsure vs ‘biden better’ or ‘trump better’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_social_security_tax_increase_support %}
Social Security Tax Increase: Respondents who said yes tax increase vs. those who said no tax increase (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_social_security_tax_increase_oppose %}
Social Security Tax Increase: This DV is the inverse of the support tax increase score above (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_social_media_user %}
Social Media Use: Respondents who said daily vs. those who said monthly or less,never (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_social_media_truth_vs_speech_truth %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_social_media_truth_vs_speech_free_speech %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_sell_federal_lands_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_sell_federal_lands_oppose %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_opioid_crisis_treat %}
Opioid Crisis Response: Respondents who said treatment vs. those who said tougher,’unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_opioid_crisis_enforce %}
Opioid Crisis Response: Respondents who said tougher vs. those who said treatment,’unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_min_wage_15_increase_support %}
Fifteen Dollar Minimum Wage: Respondents who answered ‘support’ vs those who answered ‘oppose’ or ‘unsure’ (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_min_wage_15_increase_oppose %}
Fifteen Dollar Minimum Wage: Respondents who answered ‘oppose’ vs those who answered ‘support’ or ‘unsure’ (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_mass_deporations_oppose %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_mass_deporations_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_jobs_guarantee_support %}
Jobs Guarantee: Respondents who said support vs. those who said oppose (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_jobs_guarantee_oppose %}
Jobs Guarantee: This DV is the inverse of the Jobs Guarantee support score above (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_insurance_of_last_resort_government_should_not_provide %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_insurance_of_last_resort_government_should_provide %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_immigration_undesirable %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_immigration_process_unfair %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_family_medical_leave_support %}
Family Medical Leave: Respondents who said strongly support vs. those who said strongly opposed,somewhat opposed, neutral/unsure (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_family_medical_leave_oppose %}
Family Medical Leave: Respondents who said strongly opposed,somewhat opposed vs. those who said strongly support,somewhat support,neutral/unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_state_level_fema_support %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_state_level_fema_oppose %}
Unknown: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_despondency_on_pace %}
Despondency: Respondents who answered on pace vs those who answered far ahead,a little ahead,a little behind,far behind (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_despondency_far_behind %}
Despondency: Respondents who answered far behind vs those who answered far ahead,a little ahead,on pace (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_despondency_ahead %}
Despondency: Respondents who answered ‘far ahead’ vs those who answered on pace,a little behind,far behind (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_community_college_free_support %}
Free Community College: Respondents who said yes free vs. those who said no (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_community_college_free_oppose %}
Free Community College: Respondents who said no vs. those who said yes free or unsure (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_civil_liberties_support %}
Civil Liberties: Respondents who said too restricted vs. those who said not enough (higher score = more likely to hold the stated/positive position)
{% enddocs %}

{% docs sav_hs_civil_liberties_oppose %}
Civil Liberties: This DV is the inverse of the support score above (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_candidate_mail_read_carefully %}
Candidate Mail: Respondents who answered read it carefully’ vs those who answered glance at it,no (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_candidate_mail_do_not_read %}
Candidate Mail: Respondents who answered no vs those who answered read it carefully (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_autonomous_vehicles_do_not_allow %}
Autonomous Vehicles: Respondents who said not allowed vs. those who said allowed,unsure (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_autonomous_vehicles_allow %}
Autonomous Vehicles: Respondents who said allowed vs. those who said not allowed or ‘unsure’ (higher score = more aligned with dependent variable position)
{% enddocs %}

{% docs sav_hs_aliens_governenment_hiding_much %}
Government Transparency / Aliens: field added after 2024 PDF — no guide entry (higher score = more likely to hold the opposing/negative position)
{% enddocs %}

{% docs sav_hs_aliens_governenment_disclosed_all %}
Government Transparency / Aliens: field added after 2024 PDF — no guide entry (higher score = more likely to hold the stated/positive position)
{% enddocs %}
