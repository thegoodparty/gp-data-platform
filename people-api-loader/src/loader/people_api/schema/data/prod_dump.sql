--
-- PostgreSQL database dump
--


-- Dumped from database version 16.11
-- Dumped by pg_dump version 17.10 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: Voter; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."Voter" (
    "LALVOTERID" text NOT NULL,
    "State" text NOT NULL,
    "VoterTelephones_LandlineFormatted" text,
    "VoterTelephones_LandlineConfidenceCode" integer,
    "VoterTelephones_CellPhoneFormatted" text,
    "VoterTelephones_CellConfidenceCode" integer,
    "Residence_Addresses_AddressLine" text,
    "Residence_Addresses_ExtraAddressLine" text,
    "Residence_Addresses_City" text,
    "Residence_Addresses_State" text,
    "Residence_Addresses_Zip" text,
    "Residence_Addresses_ZipPlus4" text,
    "Residence_Addresses_DPBC" text,
    "Residence_Addresses_CheckDigit" integer,
    "Residence_Addresses_HouseNumber" text,
    "Residence_Addresses_PrefixDirection" integer,
    "Residence_Addresses_StreetName" text,
    "Residence_Addresses_Designator" text,
    "Residence_Addresses_SuffixDirection" integer,
    "Residence_Addresses_ApartmentNum" text,
    "Residence_Addresses_ApartmentType" text,
    "Residence_Addresses_CassErrStatCode" text,
    "Residence_Addresses_Latitude" text,
    "Residence_Addresses_Longitude" text,
    "Residence_Addresses_LatLongAccuracy" text,
    "Residence_HHParties_Description" text,
    "Mailing_Addresses_AddressLine" text,
    "Mailing_Addresses_ExtraAddressLine" text,
    "Mailing_Addresses_City" text,
    "Mailing_Addresses_State" text,
    "Mailing_Addresses_Zip" text,
    "Mailing_Addresses_ZipPlus4" text,
    "Mailing_Addresses_DPBC" text,
    "Mailing_Addresses_CheckDigit" integer,
    "Mailing_Addresses_HouseNumber" text,
    "Mailing_Addresses_PrefixDirection" integer,
    "Mailing_Addresses_StreetName" text,
    "Mailing_Addresses_Designator" text,
    "Mailing_Addresses_SuffixDirection" integer,
    "Mailing_Addresses_ApartmentNum" text,
    "Mailing_Addresses_ApartmentType" text,
    "Mailing_Addresses_CassErrStatCode" text,
    "Mailing_Families_FamilyID" text,
    "Mailing_HHGender_Description" text,
    "Parties_Description" text,
    "VoterParties_Change_Changed_Party" text,
    "Ethnic_Description" text,
    "EthnicGroups_EthnicGroup1Desc" text,
    "CountyEthnic_LALEthnicCode" text,
    "CountyEthnic_Description" text,
    "AbsenteeTypes_Description" text,
    "General_2026" text,
    "Primary_2026" text,
    "OtherElection_2026" text,
    "AnyElection_2025" text,
    "General_2024" text,
    "Primary_2024" text,
    "PresidentialPrimary_2024" text,
    "OtherElection_2024" text,
    "AnyElection_2023" text,
    "General_2022" text,
    "Primary_2022" text,
    "OtherElection_2022" text,
    "AnyElection_2021" text,
    "General_2020" text,
    "Primary_2020" text,
    "PresidentialPrimary_2020" text,
    "OtherElection_2020" text,
    "AnyElection_2019" text,
    "General_2018" text,
    "Primary_2018" text,
    "OtherElection_2018" text,
    "AnyElection_2017" text,
    "General_2016" text,
    "Primary_2016" text,
    "PresidentialPrimary_2016" text,
    "OtherElection_2016" text,
    "US_Congressional_District" text,
    "AddressDistricts_Change_Changed_CD" text,
    "State_Senate_District" text,
    "AddressDistricts_Change_Changed_SD" text,
    "State_House_District" text,
    "AddressDistricts_Change_Changed_HD" text,
    "State_Legislative_District" text,
    "AddressDistricts_Change_Changed_LD" text,
    "County" text,
    "AddressDistricts_Change_Changed_County" text,
    "Precinct" text,
    "County_Legislative_District" text,
    "City" text,
    "City_Council_Commissioner_District" text,
    "County_Commissioner_District" text,
    "County_Supervisorial_District" text,
    "City_Mayoral_District" text,
    "Town_District" text,
    "Town_Council" text,
    "Village" text,
    "Township" text,
    "Borough" text,
    "Hamlet_Community_Area" text,
    "City_Ward" text,
    "Town_Ward" text,
    "Township_Ward" text,
    "Village_Ward" text,
    "Borough_Ward" text,
    "Board_of_Education_District" text,
    "Board_of_Education_SubDistrict" text,
    "City_School_District" text,
    "College_Board_District" text,
    "Community_College_Commissioner_District" text,
    "Community_College_SubDistrict" text,
    "County_Board_of_Education_District" text,
    "County_Board_of_Education_SubDistrict" text,
    "County_Community_College_District" text,
    "County_Superintendent_of_Schools_District" text,
    "County_Unified_School_District" text,
    "District_Attorney" text,
    "Education_Commission_District" text,
    "Educational_Service_District" text,
    "Election_Commissioner_District" text,
    "Elementary_School_District" text,
    "Elementary_School_SubDistrict" text,
    "Exempted_Village_School_District" text,
    "High_School_District" text,
    "High_School_SubDistrict" text,
    "Judicial_Appellate_District" text,
    "Judicial_Circuit_Court_District" text,
    "Judicial_County_Board_of_Review_District" text,
    "Judicial_County_Court_District" text,
    "Judicial_District" text,
    "Judicial_District_Court_District" text,
    "Judicial_Family_Court_District" text,
    "Judicial_Jury_District" text,
    "Judicial_Juvenile_Court_District" text,
    "Judicial_Magistrate_Division" text,
    "Judicial_Sub_Circuit_District" text,
    "Judicial_Superior_Court_District" text,
    "Judicial_Supreme_Court_District" text,
    "Middle_School_District" text,
    "Proposed_City_Commissioner_District" text,
    "Proposed_Elementary_School_District" text,
    "Proposed_Unified_School_District" text,
    "Regional_Office_of_Education_District" text,
    "School_Board_District" text,
    "School_District" text,
    "School_District_Vocational" text,
    "School_Facilities_Improvement_District" text,
    "School_Subdistrict" text,
    "Service_Area_District" text,
    "Superintendent_of_Schools_District" text,
    "Unified_School_District" text,
    "Unified_School_SubDistrict" text,
    "Coast_Water_District" text,
    "Consolidated_Water_District" text,
    "County_Water_District" text,
    "County_Water_Landowner_District" text,
    "County_Water_SubDistrict" text,
    "Metropolitan_Water_District" text,
    "Mountain_Water_District" text,
    "Municipal_Water_District" text,
    "Municipal_Water_SubDistrict" text,
    "River_Water_District" text,
    "Water_Agency" text,
    "Water_Agency_SubDistrict" text,
    "Water_Conservation_District" text,
    "Water_Conservation_SubDistrict" text,
    "Water_District" text,
    "Water_Public_Utility_District" text,
    "Water_Public_Utility_Subdistrict" text,
    "Water_Replacement_District" text,
    "Water_Replacement_SubDistrict" text,
    "Water_SubDistrict" text,
    "County_Fire_District" text,
    "Fire_District" text,
    "Fire_Maintenance_District" text,
    "Fire_Protection_District" text,
    "Fire_Protection_SubDistrict" text,
    "Fire_Protection_Tax_Measure_District" text,
    "Fire_Service_Area_District" text,
    "Fire_SubDistrict" text,
    "Independent_Fire_District" text,
    "Proposed_Fire_District" text,
    "Unprotected_Fire_District" text,
    "Bay_Area_Rapid_Transit" text,
    "Metro_Transit_District" text,
    "Rapid_Transit_District" text,
    "Rapid_Transit_SubDistrict" text,
    "Transit_District" text,
    "Transit_SubDistrict" text,
    "Community_Service_District" text,
    "Community_Service_SubDistrict" text,
    "County_Service_Area" text,
    "County_Service_Area_SubDistrict" text,
    "TriCity_Service_District" text,
    "Library_Services_District" text,
    "Airport_District" text,
    "Annexation_District" text,
    "Aquatic_Center_District" text,
    "Aquatic_District" text,
    "Assessment_District" text,
    "Bonds_District" text,
    "Career_Center" text,
    "Cemetery_District" text,
    "Central_Committee_District" text,
    "Chemical_Control_District" text,
    "Committee_Super_District" text,
    "Communications_District" text,
    "Community_College_At_Large" text,
    "Community_Council_District" text,
    "Community_Council_SubDistrict" text,
    "Community_Facilities_District" text,
    "Community_Facilities_SubDistrict" text,
    "Community_Hospital_District" text,
    "Community_Planning_Area" text,
    "Congressional_Township" text,
    "Conservation_District" text,
    "Conservation_SubDistrict" text,
    "Control_Zone_District" text,
    "Corrections_District" text,
    "County_Hospital_District" text,
    "County_Library_District" text,
    "County_Memorial_District" text,
    "County_Paramedic_District" text,
    "County_Sewer_District" text,
    "Democratic_Convention_Member" text,
    "Democratic_Zone" text,
    "Designated_Market_Area_DMA" text,
    "Drainage_District" text,
    "Educational_Service_Subdistrict" text,
    "Emergency_Communication_911_District" text,
    "Emergency_Communication_911_SubDistrict" text,
    "Enterprise_Zone_District" text,
    "EXT_District" text,
    "Facilities_Improvement_District" text,
    "Flood_Control_Zone" text,
    "Forest_Preserve" text,
    "Garbage_District" text,
    "Geological_Hazard_Abatement_District" text,
    "Health_District" text,
    "Hospital_SubDistrict" text,
    "Improvement_Landowner_District" text,
    "Irrigation_District" text,
    "Irrigation_SubDistrict" text,
    "Island" text,
    "Land_Commission" text,
    "Law_Enforcement_District" text,
    "Learning_Community_Coordinating_Council_District" text,
    "Levee_District" text,
    "Levee_Reconstruction_Assesment_District" text,
    "Library_District" text,
    "Library_SubDistrict" text,
    "Lighting_District" text,
    "Local_Hospital_District" text,
    "Local_Park_District" text,
    "Maintenance_District" text,
    "Master_Plan_District" text,
    "Memorial_District" text,
    "Metro_Service_District" text,
    "Metro_Service_Subdistrict" text,
    "Mosquito_Abatement_District" text,
    "Multi_township_Assessor" text,
    "Municipal_Advisory_Council_District" text,
    "Municipal_Utility_District" text,
    "Municipal_Utility_SubDistrict" text,
    "Museum_District" text,
    "Northeast_Soil_and_Water_District" text,
    "Open_Space_District" text,
    "Open_Space_SubDistrict" text,
    "Other" text,
    "Paramedic_District" text,
    "Park_Commissioner_District" text,
    "Park_District" text,
    "Park_SubDistrict" text,
    "Planning_Area_District" text,
    "Police_District" text,
    "Port_District" text,
    "Port_SubDistrict" text,
    "Power_District" text,
    "Proposed_City" text,
    "Proposed_Community_College" text,
    "Proposed_District" text,
    "Public_Airport_District" text,
    "Public_Regulation_Commission" text,
    "Public_Service_Commission_District" text,
    "Public_Utility_District" text,
    "Public_Utility_SubDistrict" text,
    "Reclamation_District" text,
    "Recreation_District" text,
    "Recreational_SubDistrict" text,
    "Republican_Area" text,
    "Republican_Convention_Member" text,
    "Resort_Improvement_District" text,
    "Resource_Conservation_District" text,
    "Road_Maintenance_District" text,
    "Rural_Service_District" text,
    "Sanitary_District" text,
    "Sanitary_SubDistrict" text,
    "Sewer_District" text,
    "Sewer_Maintenance_District" text,
    "Sewer_SubDistrict" text,
    "Snow_Removal_District" text,
    "Special_Reporting_District" text,
    "Special_Tax_District" text,
    "Storm_Water_District" text,
    "Street_Lighting_District" text,
    "TV_Translator_District" text,
    "Unincorporated_District" text,
    "Unincorporated_Park_District" text,
    "Ute_Creek_Soil_District" text,
    "Vector_Control_District" text,
    "Vote_By_Mail_Area" text,
    "Wastewater_District" text,
    "Weed_District" text,
    "Active" text,
    "Age" text,
    "Age_Int" integer,
    "CalculatedRegDate" date,
    "CountyVoterID" text,
    "FIPS" text,
    "FirstName" text,
    "Gender" text,
    "LastName" text,
    "MiddleName" text,
    "MovedFrom_Date" date,
    "MovedFrom_Party_Description" text,
    "MovedFrom_State" text,
    "NameSuffix" text,
    "OfficialRegDate" text,
    "PlaceOfBirth" text,
    "SequenceOddEven" text,
    "SequenceZigZag" text,
    "StateVoterID" text,
    "VotingPerformanceEvenYearGeneral" text,
    "VotingPerformanceEvenYearGeneralAndPrimary" text,
    "VotingPerformanceEvenYearPrimary" text,
    "VotingPerformanceMinorElection" text,
    created_at timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id uuid NOT NULL,
    updated_at timestamp(3) without time zone NOT NULL,
    "Business_Owner" text,
    "Education_Of_Person" text,
    "Estimated_Income_Amount" text,
    "Homeowner_Probability_Model" text,
    "Judicial_Municipal_Court_District" text,
    "Landscaping_and_Lighting_Assessment_District" text,
    "Language_Code" text,
    "Marital_Status" text,
    "Presence_Of_Children" text,
    "Veteran_Status" text,
    "Voter_Status" text,
    "Voter_Status_UpdatedAt" timestamp(3) without time zone,
    "Water_Control_Water_Conservation" text,
    "Water_Control_Water_Conservation_SubDistrict" text,
    "Estimated_Income_Amount_Int" integer
);


--
-- Name: Voter Voter_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Voter"
    ADD CONSTRAINT "Voter_pkey" PRIMARY KEY (id);


--
-- Name: Voter_Active_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");


--
-- Name: Voter_Age_Int_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Age_Int_idx" ON public."Voter" USING btree ("Age_Int");


--
-- Name: Voter_Age_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Age_idx" ON public."Voter" USING btree ("Age");


--
-- Name: Voter_Airport_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Airport_District_idx" ON public."Voter" USING btree ("Airport_District");


--
-- Name: Voter_Annexation_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Annexation_District_idx" ON public."Voter" USING btree ("Annexation_District");


--
-- Name: Voter_Aquatic_Center_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Aquatic_Center_District_idx" ON public."Voter" USING btree ("Aquatic_Center_District");


--
-- Name: Voter_Aquatic_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Aquatic_District_idx" ON public."Voter" USING btree ("Aquatic_District");


--
-- Name: Voter_Assessment_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Assessment_District_idx" ON public."Voter" USING btree ("Assessment_District");


--
-- Name: Voter_Bay_Area_Rapid_Transit_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Bay_Area_Rapid_Transit_idx" ON public."Voter" USING btree ("Bay_Area_Rapid_Transit");


--
-- Name: Voter_Board_of_Education_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Board_of_Education_District_idx" ON public."Voter" USING btree ("Board_of_Education_District");


--
-- Name: Voter_Board_of_Education_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Board_of_Education_SubDistrict_idx" ON public."Voter" USING btree ("Board_of_Education_SubDistrict");


--
-- Name: Voter_Bonds_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Bonds_District_idx" ON public."Voter" USING btree ("Bonds_District");


--
-- Name: Voter_Borough_Ward_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Borough_Ward_idx" ON public."Voter" USING btree ("Borough_Ward");


--
-- Name: Voter_Borough_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Borough_idx" ON public."Voter" USING btree ("Borough");


--
-- Name: Voter_Business_Owner_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Business_Owner_idx" ON public."Voter" USING btree ("Business_Owner");


--
-- Name: Voter_CalculatedRegDate_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_CalculatedRegDate_idx" ON public."Voter" USING btree ("CalculatedRegDate");


--
-- Name: Voter_Career_Center_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Career_Center_idx" ON public."Voter" USING btree ("Career_Center");


--
-- Name: Voter_Cemetery_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Cemetery_District_idx" ON public."Voter" USING btree ("Cemetery_District");


--
-- Name: Voter_Central_Committee_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Central_Committee_District_idx" ON public."Voter" USING btree ("Central_Committee_District");


--
-- Name: Voter_Chemical_Control_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Chemical_Control_District_idx" ON public."Voter" USING btree ("Chemical_Control_District");


--
-- Name: Voter_City_Council_Commissioner_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_City_Council_Commissioner_District_idx" ON public."Voter" USING btree ("City_Council_Commissioner_District");


--
-- Name: Voter_City_Mayoral_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_City_Mayoral_District_idx" ON public."Voter" USING btree ("City_Mayoral_District");


--
-- Name: Voter_City_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_City_School_District_idx" ON public."Voter" USING btree ("City_School_District");


--
-- Name: Voter_City_Ward_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_City_Ward_idx" ON public."Voter" USING btree ("City_Ward");


--
-- Name: Voter_City_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_City_idx" ON public."Voter" USING btree ("City");


--
-- Name: Voter_Coast_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Coast_Water_District_idx" ON public."Voter" USING btree ("Coast_Water_District");


--
-- Name: Voter_College_Board_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_College_Board_District_idx" ON public."Voter" USING btree ("College_Board_District");


--
-- Name: Voter_Committee_Super_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Committee_Super_District_idx" ON public."Voter" USING btree ("Committee_Super_District");


--
-- Name: Voter_Communications_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Communications_District_idx" ON public."Voter" USING btree ("Communications_District");


--
-- Name: Voter_Community_College_At_Large_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_College_At_Large_idx" ON public."Voter" USING btree ("Community_College_At_Large");


--
-- Name: Voter_Community_College_Commissioner_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_College_Commissioner_District_idx" ON public."Voter" USING btree ("Community_College_Commissioner_District");


--
-- Name: Voter_Community_College_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_College_SubDistrict_idx" ON public."Voter" USING btree ("Community_College_SubDistrict");


--
-- Name: Voter_Community_Council_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_Council_District_idx" ON public."Voter" USING btree ("Community_Council_District");


--
-- Name: Voter_Community_Council_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_Council_SubDistrict_idx" ON public."Voter" USING btree ("Community_Council_SubDistrict");


--
-- Name: Voter_Community_Facilities_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_Facilities_District_idx" ON public."Voter" USING btree ("Community_Facilities_District");


--
-- Name: Voter_Community_Facilities_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_Facilities_SubDistrict_idx" ON public."Voter" USING btree ("Community_Facilities_SubDistrict");


--
-- Name: Voter_Community_Hospital_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_Hospital_District_idx" ON public."Voter" USING btree ("Community_Hospital_District");


--
-- Name: Voter_Community_Planning_Area_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_Planning_Area_idx" ON public."Voter" USING btree ("Community_Planning_Area");


--
-- Name: Voter_Community_Service_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_Service_District_idx" ON public."Voter" USING btree ("Community_Service_District");


--
-- Name: Voter_Community_Service_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Community_Service_SubDistrict_idx" ON public."Voter" USING btree ("Community_Service_SubDistrict");


--
-- Name: Voter_Congressional_Township_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Congressional_Township_idx" ON public."Voter" USING btree ("Congressional_Township");


--
-- Name: Voter_Consolidated_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Consolidated_Water_District_idx" ON public."Voter" USING btree ("Consolidated_Water_District");


--
-- Name: Voter_Control_Zone_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Control_Zone_District_idx" ON public."Voter" USING btree ("Control_Zone_District");


--
-- Name: Voter_Corrections_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Corrections_District_idx" ON public."Voter" USING btree ("Corrections_District");


--
-- Name: Voter_County_Board_of_Education_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Board_of_Education_District_idx" ON public."Voter" USING btree ("County_Board_of_Education_District");


--
-- Name: Voter_County_Board_of_Education_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Board_of_Education_SubDistrict_idx" ON public."Voter" USING btree ("County_Board_of_Education_SubDistrict");


--
-- Name: Voter_County_Commissioner_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Commissioner_District_idx" ON public."Voter" USING btree ("County_Commissioner_District");


--
-- Name: Voter_County_Community_College_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Community_College_District_idx" ON public."Voter" USING btree ("County_Community_College_District");


--
-- Name: Voter_County_Fire_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Fire_District_idx" ON public."Voter" USING btree ("County_Fire_District");


--
-- Name: Voter_County_Hospital_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Hospital_District_idx" ON public."Voter" USING btree ("County_Hospital_District");


--
-- Name: Voter_County_Legislative_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Legislative_District_idx" ON public."Voter" USING btree ("County_Legislative_District");


--
-- Name: Voter_County_Library_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Library_District_idx" ON public."Voter" USING btree ("County_Library_District");


--
-- Name: Voter_County_Memorial_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Memorial_District_idx" ON public."Voter" USING btree ("County_Memorial_District");


--
-- Name: Voter_County_Paramedic_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Paramedic_District_idx" ON public."Voter" USING btree ("County_Paramedic_District");


--
-- Name: Voter_County_Service_Area_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Service_Area_SubDistrict_idx" ON public."Voter" USING btree ("County_Service_Area_SubDistrict");


--
-- Name: Voter_County_Service_Area_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Service_Area_idx" ON public."Voter" USING btree ("County_Service_Area");


--
-- Name: Voter_County_Sewer_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Sewer_District_idx" ON public."Voter" USING btree ("County_Sewer_District");


--
-- Name: Voter_County_Superintendent_of_Schools_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Superintendent_of_Schools_District_idx" ON public."Voter" USING btree ("County_Superintendent_of_Schools_District");


--
-- Name: Voter_County_Supervisorial_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Supervisorial_District_idx" ON public."Voter" USING btree ("County_Supervisorial_District");


--
-- Name: Voter_County_Unified_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Unified_School_District_idx" ON public."Voter" USING btree ("County_Unified_School_District");


--
-- Name: Voter_County_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Water_District_idx" ON public."Voter" USING btree ("County_Water_District");


--
-- Name: Voter_County_Water_Landowner_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Water_Landowner_District_idx" ON public."Voter" USING btree ("County_Water_Landowner_District");


--
-- Name: Voter_County_Water_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_Water_SubDistrict_idx" ON public."Voter" USING btree ("County_Water_SubDistrict");


--
-- Name: Voter_County_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_County_idx" ON public."Voter" USING btree ("County");


--
-- Name: Voter_Democratic_Convention_Member_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Democratic_Convention_Member_idx" ON public."Voter" USING btree ("Democratic_Convention_Member");


--
-- Name: Voter_Democratic_Zone_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Democratic_Zone_idx" ON public."Voter" USING btree ("Democratic_Zone");


--
-- Name: Voter_Designated_Market_Area_DMA_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Designated_Market_Area_DMA_idx" ON public."Voter" USING btree ("Designated_Market_Area_DMA");


--
-- Name: Voter_District_Attorney_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_District_Attorney_idx" ON public."Voter" USING btree ("District_Attorney");


--
-- Name: Voter_Drainage_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Drainage_District_idx" ON public."Voter" USING btree ("Drainage_District");


--
-- Name: Voter_EXT_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_EXT_District_idx" ON public."Voter" USING btree ("EXT_District");


--
-- Name: Voter_Education_Commission_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Education_Commission_District_idx" ON public."Voter" USING btree ("Education_Commission_District");


--
-- Name: Voter_Education_Of_Person_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Education_Of_Person_idx" ON public."Voter" USING btree ("Education_Of_Person");


--
-- Name: Voter_Educational_Service_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Educational_Service_District_idx" ON public."Voter" USING btree ("Educational_Service_District");


--
-- Name: Voter_Educational_Service_Subdistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Educational_Service_Subdistrict_idx" ON public."Voter" USING btree ("Educational_Service_Subdistrict");


--
-- Name: Voter_Election_Commissioner_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Election_Commissioner_District_idx" ON public."Voter" USING btree ("Election_Commissioner_District");


--
-- Name: Voter_Elementary_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Elementary_School_District_idx" ON public."Voter" USING btree ("Elementary_School_District");


--
-- Name: Voter_Elementary_School_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Elementary_School_SubDistrict_idx" ON public."Voter" USING btree ("Elementary_School_SubDistrict");


--
-- Name: Voter_Emergency_Communication_911_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Emergency_Communication_911_District_idx" ON public."Voter" USING btree ("Emergency_Communication_911_District");


--
-- Name: Voter_Emergency_Communication_911_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Emergency_Communication_911_SubDistrict_idx" ON public."Voter" USING btree ("Emergency_Communication_911_SubDistrict");


--
-- Name: Voter_Enterprise_Zone_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Enterprise_Zone_District_idx" ON public."Voter" USING btree ("Enterprise_Zone_District");


--
-- Name: Voter_Estimated_Income_Amount_Int_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Estimated_Income_Amount_Int_idx" ON public."Voter" USING btree ("Estimated_Income_Amount_Int");


--
-- Name: Voter_Estimated_Income_Amount_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Estimated_Income_Amount_idx" ON public."Voter" USING btree ("Estimated_Income_Amount");


--
-- Name: Voter_EthnicGroups_EthnicGroup1Desc_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_EthnicGroups_EthnicGroup1Desc_idx" ON public."Voter" USING btree ("EthnicGroups_EthnicGroup1Desc");


--
-- Name: Voter_Exempted_Village_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Exempted_Village_School_District_idx" ON public."Voter" USING btree ("Exempted_Village_School_District");


--
-- Name: Voter_Facilities_Improvement_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Facilities_Improvement_District_idx" ON public."Voter" USING btree ("Facilities_Improvement_District");


--
-- Name: Voter_Fire_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Fire_District_idx" ON public."Voter" USING btree ("Fire_District");


--
-- Name: Voter_Fire_Maintenance_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Fire_Maintenance_District_idx" ON public."Voter" USING btree ("Fire_Maintenance_District");


--
-- Name: Voter_Fire_Protection_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Fire_Protection_District_idx" ON public."Voter" USING btree ("Fire_Protection_District");


--
-- Name: Voter_Fire_Protection_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Fire_Protection_SubDistrict_idx" ON public."Voter" USING btree ("Fire_Protection_SubDistrict");


--
-- Name: Voter_Fire_Protection_Tax_Measure_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Fire_Protection_Tax_Measure_District_idx" ON public."Voter" USING btree ("Fire_Protection_Tax_Measure_District");


--
-- Name: Voter_Fire_Service_Area_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Fire_Service_Area_District_idx" ON public."Voter" USING btree ("Fire_Service_Area_District");


--
-- Name: Voter_Fire_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Fire_SubDistrict_idx" ON public."Voter" USING btree ("Fire_SubDistrict");


--
-- Name: Voter_FirstName_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_FirstName_idx" ON public."Voter" USING btree ("FirstName");


--
-- Name: Voter_Flood_Control_Zone_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Flood_Control_Zone_idx" ON public."Voter" USING btree ("Flood_Control_Zone");


--
-- Name: Voter_Forest_Preserve_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Forest_Preserve_idx" ON public."Voter" USING btree ("Forest_Preserve");


--
-- Name: Voter_Garbage_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Garbage_District_idx" ON public."Voter" USING btree ("Garbage_District");


--
-- Name: Voter_Gender_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Gender_idx" ON public."Voter" USING btree ("Gender");


--
-- Name: Voter_Geological_Hazard_Abatement_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Geological_Hazard_Abatement_District_idx" ON public."Voter" USING btree ("Geological_Hazard_Abatement_District");


--
-- Name: Voter_Hamlet_Community_Area_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Hamlet_Community_Area_idx" ON public."Voter" USING btree ("Hamlet_Community_Area");


--
-- Name: Voter_Health_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Health_District_idx" ON public."Voter" USING btree ("Health_District");


--
-- Name: Voter_High_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_High_School_District_idx" ON public."Voter" USING btree ("High_School_District");


--
-- Name: Voter_High_School_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_High_School_SubDistrict_idx" ON public."Voter" USING btree ("High_School_SubDistrict");


--
-- Name: Voter_Homeowner_Probability_Model_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Homeowner_Probability_Model_idx" ON public."Voter" USING btree ("Homeowner_Probability_Model");


--
-- Name: Voter_Hospital_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Hospital_SubDistrict_idx" ON public."Voter" USING btree ("Hospital_SubDistrict");


--
-- Name: Voter_Improvement_Landowner_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Improvement_Landowner_District_idx" ON public."Voter" USING btree ("Improvement_Landowner_District");


--
-- Name: Voter_Independent_Fire_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Independent_Fire_District_idx" ON public."Voter" USING btree ("Independent_Fire_District");


--
-- Name: Voter_Irrigation_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Irrigation_District_idx" ON public."Voter" USING btree ("Irrigation_District");


--
-- Name: Voter_Irrigation_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Irrigation_SubDistrict_idx" ON public."Voter" USING btree ("Irrigation_SubDistrict");


--
-- Name: Voter_Island_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Island_idx" ON public."Voter" USING btree ("Island");


--
-- Name: Voter_Judicial_Appellate_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Appellate_District_idx" ON public."Voter" USING btree ("Judicial_Appellate_District");


--
-- Name: Voter_Judicial_Circuit_Court_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Circuit_Court_District_idx" ON public."Voter" USING btree ("Judicial_Circuit_Court_District");


--
-- Name: Voter_Judicial_County_Board_of_Review_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_County_Board_of_Review_District_idx" ON public."Voter" USING btree ("Judicial_County_Board_of_Review_District");


--
-- Name: Voter_Judicial_County_Court_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_County_Court_District_idx" ON public."Voter" USING btree ("Judicial_County_Court_District");


--
-- Name: Voter_Judicial_District_Court_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_District_Court_District_idx" ON public."Voter" USING btree ("Judicial_District_Court_District");


--
-- Name: Voter_Judicial_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_District_idx" ON public."Voter" USING btree ("Judicial_District");


--
-- Name: Voter_Judicial_Family_Court_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Family_Court_District_idx" ON public."Voter" USING btree ("Judicial_Family_Court_District");


--
-- Name: Voter_Judicial_Jury_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Jury_District_idx" ON public."Voter" USING btree ("Judicial_Jury_District");


--
-- Name: Voter_Judicial_Juvenile_Court_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Juvenile_Court_District_idx" ON public."Voter" USING btree ("Judicial_Juvenile_Court_District");


--
-- Name: Voter_Judicial_Magistrate_Division_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Magistrate_Division_idx" ON public."Voter" USING btree ("Judicial_Magistrate_Division");


--
-- Name: Voter_Judicial_Municipal_Court_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Municipal_Court_District_idx" ON public."Voter" USING btree ("Judicial_Municipal_Court_District");


--
-- Name: Voter_Judicial_Sub_Circuit_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Sub_Circuit_District_idx" ON public."Voter" USING btree ("Judicial_Sub_Circuit_District");


--
-- Name: Voter_Judicial_Superior_Court_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Superior_Court_District_idx" ON public."Voter" USING btree ("Judicial_Superior_Court_District");


--
-- Name: Voter_Judicial_Supreme_Court_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Judicial_Supreme_Court_District_idx" ON public."Voter" USING btree ("Judicial_Supreme_Court_District");


--
-- Name: Voter_LALVOTERID_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX "Voter_LALVOTERID_key" ON public."Voter" USING btree ("LALVOTERID");


--
-- Name: Voter_Land_Commission_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Land_Commission_idx" ON public."Voter" USING btree ("Land_Commission");


--
-- Name: Voter_Landscaping_and_Lighting_Assessment_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Landscaping_and_Lighting_Assessment_District_idx" ON public."Voter" USING btree ("Landscaping_and_Lighting_Assessment_District");


--
-- Name: Voter_Language_Code_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Language_Code_idx" ON public."Voter" USING btree ("Language_Code");


--
-- Name: Voter_LastName_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_LastName_idx" ON public."Voter" USING btree ("LastName");


--
-- Name: Voter_Law_Enforcement_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Law_Enforcement_District_idx" ON public."Voter" USING btree ("Law_Enforcement_District");


--
-- Name: Voter_Learning_Community_Coordinating_Council_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Learning_Community_Coordinating_Council_District_idx" ON public."Voter" USING btree ("Learning_Community_Coordinating_Council_District");


--
-- Name: Voter_Levee_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Levee_District_idx" ON public."Voter" USING btree ("Levee_District");


--
-- Name: Voter_Levee_Reconstruction_Assesment_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Levee_Reconstruction_Assesment_District_idx" ON public."Voter" USING btree ("Levee_Reconstruction_Assesment_District");


--
-- Name: Voter_Library_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Library_District_idx" ON public."Voter" USING btree ("Library_District");


--
-- Name: Voter_Library_Services_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Library_Services_District_idx" ON public."Voter" USING btree ("Library_Services_District");


--
-- Name: Voter_Library_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Library_SubDistrict_idx" ON public."Voter" USING btree ("Library_SubDistrict");


--
-- Name: Voter_Lighting_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Lighting_District_idx" ON public."Voter" USING btree ("Lighting_District");


--
-- Name: Voter_Local_Hospital_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Local_Hospital_District_idx" ON public."Voter" USING btree ("Local_Hospital_District");


--
-- Name: Voter_Local_Park_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Local_Park_District_idx" ON public."Voter" USING btree ("Local_Park_District");


--
-- Name: Voter_Mailing_Families_FamilyID_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Mailing_Families_FamilyID_idx" ON public."Voter" USING btree ("Mailing_Families_FamilyID");


--
-- Name: Voter_Maintenance_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Maintenance_District_idx" ON public."Voter" USING btree ("Maintenance_District");


--
-- Name: Voter_Marital_Status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Marital_Status_idx" ON public."Voter" USING btree ("Marital_Status");


--
-- Name: Voter_Master_Plan_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Master_Plan_District_idx" ON public."Voter" USING btree ("Master_Plan_District");


--
-- Name: Voter_Memorial_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Memorial_District_idx" ON public."Voter" USING btree ("Memorial_District");


--
-- Name: Voter_Metro_Service_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Metro_Service_District_idx" ON public."Voter" USING btree ("Metro_Service_District");


--
-- Name: Voter_Metro_Service_Subdistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Metro_Service_Subdistrict_idx" ON public."Voter" USING btree ("Metro_Service_Subdistrict");


--
-- Name: Voter_Metro_Transit_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Metro_Transit_District_idx" ON public."Voter" USING btree ("Metro_Transit_District");


--
-- Name: Voter_Metropolitan_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Metropolitan_Water_District_idx" ON public."Voter" USING btree ("Metropolitan_Water_District");


--
-- Name: Voter_MiddleName_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_MiddleName_idx" ON public."Voter" USING btree ("MiddleName");


--
-- Name: Voter_Middle_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Middle_School_District_idx" ON public."Voter" USING btree ("Middle_School_District");


--
-- Name: Voter_Mosquito_Abatement_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Mosquito_Abatement_District_idx" ON public."Voter" USING btree ("Mosquito_Abatement_District");


--
-- Name: Voter_Mountain_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Mountain_Water_District_idx" ON public."Voter" USING btree ("Mountain_Water_District");


--
-- Name: Voter_Multi_township_Assessor_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Multi_township_Assessor_idx" ON public."Voter" USING btree ("Multi_township_Assessor");


--
-- Name: Voter_Municipal_Advisory_Council_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Municipal_Advisory_Council_District_idx" ON public."Voter" USING btree ("Municipal_Advisory_Council_District");


--
-- Name: Voter_Municipal_Utility_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Municipal_Utility_District_idx" ON public."Voter" USING btree ("Municipal_Utility_District");


--
-- Name: Voter_Municipal_Utility_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Municipal_Utility_SubDistrict_idx" ON public."Voter" USING btree ("Municipal_Utility_SubDistrict");


--
-- Name: Voter_Municipal_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Municipal_Water_District_idx" ON public."Voter" USING btree ("Municipal_Water_District");


--
-- Name: Voter_Municipal_Water_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Municipal_Water_SubDistrict_idx" ON public."Voter" USING btree ("Municipal_Water_SubDistrict");


--
-- Name: Voter_Museum_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Museum_District_idx" ON public."Voter" USING btree ("Museum_District");


--
-- Name: Voter_Northeast_Soil_and_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Northeast_Soil_and_Water_District_idx" ON public."Voter" USING btree ("Northeast_Soil_and_Water_District");


--
-- Name: Voter_OfficialRegDate_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_OfficialRegDate_idx" ON public."Voter" USING btree ("OfficialRegDate");


--
-- Name: Voter_Open_Space_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Open_Space_District_idx" ON public."Voter" USING btree ("Open_Space_District");


--
-- Name: Voter_Open_Space_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Open_Space_SubDistrict_idx" ON public."Voter" USING btree ("Open_Space_SubDistrict");


--
-- Name: Voter_Other_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Other_idx" ON public."Voter" USING btree ("Other");


--
-- Name: Voter_Paramedic_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Paramedic_District_idx" ON public."Voter" USING btree ("Paramedic_District");


--
-- Name: Voter_Park_Commissioner_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Park_Commissioner_District_idx" ON public."Voter" USING btree ("Park_Commissioner_District");


--
-- Name: Voter_Park_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Park_District_idx" ON public."Voter" USING btree ("Park_District");


--
-- Name: Voter_Park_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Park_SubDistrict_idx" ON public."Voter" USING btree ("Park_SubDistrict");


--
-- Name: Voter_Parties_Description_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Parties_Description_idx" ON public."Voter" USING btree ("Parties_Description");


--
-- Name: Voter_Planning_Area_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Planning_Area_District_idx" ON public."Voter" USING btree ("Planning_Area_District");


--
-- Name: Voter_Police_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Police_District_idx" ON public."Voter" USING btree ("Police_District");


--
-- Name: Voter_Port_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Port_District_idx" ON public."Voter" USING btree ("Port_District");


--
-- Name: Voter_Port_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Port_SubDistrict_idx" ON public."Voter" USING btree ("Port_SubDistrict");


--
-- Name: Voter_Power_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Power_District_idx" ON public."Voter" USING btree ("Power_District");


--
-- Name: Voter_Precinct_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Precinct_idx" ON public."Voter" USING btree ("Precinct");


--
-- Name: Voter_Presence_Of_Children_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Presence_Of_Children_idx" ON public."Voter" USING btree ("Presence_Of_Children");


--
-- Name: Voter_Proposed_City_Commissioner_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Proposed_City_Commissioner_District_idx" ON public."Voter" USING btree ("Proposed_City_Commissioner_District");


--
-- Name: Voter_Proposed_City_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Proposed_City_idx" ON public."Voter" USING btree ("Proposed_City");


--
-- Name: Voter_Proposed_Community_College_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Proposed_Community_College_idx" ON public."Voter" USING btree ("Proposed_Community_College");


--
-- Name: Voter_Proposed_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Proposed_District_idx" ON public."Voter" USING btree ("Proposed_District");


--
-- Name: Voter_Proposed_Elementary_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Proposed_Elementary_School_District_idx" ON public."Voter" USING btree ("Proposed_Elementary_School_District");


--
-- Name: Voter_Proposed_Fire_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Proposed_Fire_District_idx" ON public."Voter" USING btree ("Proposed_Fire_District");


--
-- Name: Voter_Proposed_Unified_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Proposed_Unified_School_District_idx" ON public."Voter" USING btree ("Proposed_Unified_School_District");


--
-- Name: Voter_Public_Airport_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Public_Airport_District_idx" ON public."Voter" USING btree ("Public_Airport_District");


--
-- Name: Voter_Public_Regulation_Commission_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Public_Regulation_Commission_idx" ON public."Voter" USING btree ("Public_Regulation_Commission");


--
-- Name: Voter_Public_Service_Commission_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Public_Service_Commission_District_idx" ON public."Voter" USING btree ("Public_Service_Commission_District");


--
-- Name: Voter_Public_Utility_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Public_Utility_District_idx" ON public."Voter" USING btree ("Public_Utility_District");


--
-- Name: Voter_Public_Utility_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Public_Utility_SubDistrict_idx" ON public."Voter" USING btree ("Public_Utility_SubDistrict");


--
-- Name: Voter_Rapid_Transit_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Rapid_Transit_District_idx" ON public."Voter" USING btree ("Rapid_Transit_District");


--
-- Name: Voter_Rapid_Transit_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Rapid_Transit_SubDistrict_idx" ON public."Voter" USING btree ("Rapid_Transit_SubDistrict");


--
-- Name: Voter_Reclamation_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Reclamation_District_idx" ON public."Voter" USING btree ("Reclamation_District");


--
-- Name: Voter_Recreation_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Recreation_District_idx" ON public."Voter" USING btree ("Recreation_District");


--
-- Name: Voter_Recreational_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Recreational_SubDistrict_idx" ON public."Voter" USING btree ("Recreational_SubDistrict");


--
-- Name: Voter_Regional_Office_of_Education_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Regional_Office_of_Education_District_idx" ON public."Voter" USING btree ("Regional_Office_of_Education_District");


--
-- Name: Voter_Republican_Area_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Republican_Area_idx" ON public."Voter" USING btree ("Republican_Area");


--
-- Name: Voter_Republican_Convention_Member_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Republican_Convention_Member_idx" ON public."Voter" USING btree ("Republican_Convention_Member");


--
-- Name: Voter_Resort_Improvement_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Resort_Improvement_District_idx" ON public."Voter" USING btree ("Resort_Improvement_District");


--
-- Name: Voter_Resource_Conservation_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Resource_Conservation_District_idx" ON public."Voter" USING btree ("Resource_Conservation_District");


--
-- Name: Voter_River_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_River_Water_District_idx" ON public."Voter" USING btree ("River_Water_District");


--
-- Name: Voter_Road_Maintenance_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Road_Maintenance_District_idx" ON public."Voter" USING btree ("Road_Maintenance_District");


--
-- Name: Voter_Rural_Service_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Rural_Service_District_idx" ON public."Voter" USING btree ("Rural_Service_District");


--
-- Name: Voter_Sanitary_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Sanitary_District_idx" ON public."Voter" USING btree ("Sanitary_District");


--
-- Name: Voter_Sanitary_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Sanitary_SubDistrict_idx" ON public."Voter" USING btree ("Sanitary_SubDistrict");


--
-- Name: Voter_School_Board_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_School_Board_District_idx" ON public."Voter" USING btree ("School_Board_District");


--
-- Name: Voter_School_District_Vocational_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_School_District_Vocational_idx" ON public."Voter" USING btree ("School_District_Vocational");


--
-- Name: Voter_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_School_District_idx" ON public."Voter" USING btree ("School_District");


--
-- Name: Voter_School_Facilities_Improvement_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_School_Facilities_Improvement_District_idx" ON public."Voter" USING btree ("School_Facilities_Improvement_District");


--
-- Name: Voter_School_Subdistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_School_Subdistrict_idx" ON public."Voter" USING btree ("School_Subdistrict");


--
-- Name: Voter_Service_Area_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Service_Area_District_idx" ON public."Voter" USING btree ("Service_Area_District");


--
-- Name: Voter_Sewer_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Sewer_District_idx" ON public."Voter" USING btree ("Sewer_District");


--
-- Name: Voter_Sewer_Maintenance_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Sewer_Maintenance_District_idx" ON public."Voter" USING btree ("Sewer_Maintenance_District");


--
-- Name: Voter_Sewer_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Sewer_SubDistrict_idx" ON public."Voter" USING btree ("Sewer_SubDistrict");


--
-- Name: Voter_Snow_Removal_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Snow_Removal_District_idx" ON public."Voter" USING btree ("Snow_Removal_District");


--
-- Name: Voter_Special_Reporting_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Special_Reporting_District_idx" ON public."Voter" USING btree ("Special_Reporting_District");


--
-- Name: Voter_Special_Tax_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Special_Tax_District_idx" ON public."Voter" USING btree ("Special_Tax_District");


--
-- Name: Voter_State_House_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_State_House_District_idx" ON public."Voter" USING btree ("State_House_District");


--
-- Name: Voter_State_Senate_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_State_Senate_District_idx" ON public."Voter" USING btree ("State_Senate_District");


--
-- Name: Voter_State_updated_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_State_updated_at_idx" ON public."Voter" USING btree ("State", updated_at);


--
-- Name: Voter_Storm_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Storm_Water_District_idx" ON public."Voter" USING btree ("Storm_Water_District");


--
-- Name: Voter_Street_Lighting_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Street_Lighting_District_idx" ON public."Voter" USING btree ("Street_Lighting_District");


--
-- Name: Voter_Superintendent_of_Schools_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Superintendent_of_Schools_District_idx" ON public."Voter" USING btree ("Superintendent_of_Schools_District");


--
-- Name: Voter_TV_Translator_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_TV_Translator_District_idx" ON public."Voter" USING btree ("TV_Translator_District");


--
-- Name: Voter_Town_Council_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Town_Council_idx" ON public."Voter" USING btree ("Town_Council");


--
-- Name: Voter_Town_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Town_District_idx" ON public."Voter" USING btree ("Town_District");


--
-- Name: Voter_Town_Ward_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Town_Ward_idx" ON public."Voter" USING btree ("Town_Ward");


--
-- Name: Voter_Township_Ward_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Township_Ward_idx" ON public."Voter" USING btree ("Township_Ward");


--
-- Name: Voter_Township_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Township_idx" ON public."Voter" USING btree ("Township");


--
-- Name: Voter_Transit_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Transit_District_idx" ON public."Voter" USING btree ("Transit_District");


--
-- Name: Voter_Transit_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Transit_SubDistrict_idx" ON public."Voter" USING btree ("Transit_SubDistrict");


--
-- Name: Voter_TriCity_Service_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_TriCity_Service_District_idx" ON public."Voter" USING btree ("TriCity_Service_District");


--
-- Name: Voter_US_Congressional_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_US_Congressional_District_idx" ON public."Voter" USING btree ("US_Congressional_District");


--
-- Name: Voter_Unified_School_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Unified_School_District_idx" ON public."Voter" USING btree ("Unified_School_District");


--
-- Name: Voter_Unified_School_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Unified_School_SubDistrict_idx" ON public."Voter" USING btree ("Unified_School_SubDistrict");


--
-- Name: Voter_Unincorporated_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Unincorporated_District_idx" ON public."Voter" USING btree ("Unincorporated_District");


--
-- Name: Voter_Unincorporated_Park_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Unincorporated_Park_District_idx" ON public."Voter" USING btree ("Unincorporated_Park_District");


--
-- Name: Voter_Unprotected_Fire_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Unprotected_Fire_District_idx" ON public."Voter" USING btree ("Unprotected_Fire_District");


--
-- Name: Voter_Ute_Creek_Soil_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Ute_Creek_Soil_District_idx" ON public."Voter" USING btree ("Ute_Creek_Soil_District");


--
-- Name: Voter_Vector_Control_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Vector_Control_District_idx" ON public."Voter" USING btree ("Vector_Control_District");


--
-- Name: Voter_Veteran_Status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Veteran_Status_idx" ON public."Voter" USING btree ("Veteran_Status");


--
-- Name: Voter_Village_Ward_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Village_Ward_idx" ON public."Voter" USING btree ("Village_Ward");


--
-- Name: Voter_Village_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Village_idx" ON public."Voter" USING btree ("Village");


--
-- Name: Voter_Vote_By_Mail_Area_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Vote_By_Mail_Area_idx" ON public."Voter" USING btree ("Vote_By_Mail_Area");


--
-- Name: Voter_VoterParties_Change_Changed_Party_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VoterParties_Change_Changed_Party_idx" ON public."Voter" USING btree ("VoterParties_Change_Changed_Party");


--
-- Name: Voter_VoterTelephones_CellConfidenceCode_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VoterTelephones_CellConfidenceCode_idx" ON public."Voter" USING btree ("VoterTelephones_CellConfidenceCode");


--
-- Name: Voter_VoterTelephones_CellPhoneFormatted_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VoterTelephones_CellPhoneFormatted_idx" ON public."Voter" USING btree ("VoterTelephones_CellPhoneFormatted");


--
-- Name: Voter_VoterTelephones_LandlineConfidenceCode_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VoterTelephones_LandlineConfidenceCode_idx" ON public."Voter" USING btree ("VoterTelephones_LandlineConfidenceCode");


--
-- Name: Voter_VoterTelephones_LandlineFormatted_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VoterTelephones_LandlineFormatted_idx" ON public."Voter" USING btree ("VoterTelephones_LandlineFormatted");


--
-- Name: Voter_Voter_Status_UpdatedAt_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Voter_Status_UpdatedAt_idx" ON public."Voter" USING btree ("Voter_Status_UpdatedAt");


--
-- Name: Voter_Voter_Status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Voter_Status_idx" ON public."Voter" USING btree ("Voter_Status");


--
-- Name: Voter_VotingPerformanceEvenYearGeneralAndPrimary_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VotingPerformanceEvenYearGeneralAndPrimary_idx" ON public."Voter" USING btree ("VotingPerformanceEvenYearGeneralAndPrimary");


--
-- Name: Voter_VotingPerformanceEvenYearGeneral_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VotingPerformanceEvenYearGeneral_idx" ON public."Voter" USING btree ("VotingPerformanceEvenYearGeneral");


--
-- Name: Voter_VotingPerformanceEvenYearPrimary_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VotingPerformanceEvenYearPrimary_idx" ON public."Voter" USING btree ("VotingPerformanceEvenYearPrimary");


--
-- Name: Voter_VotingPerformanceMinorElection_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_VotingPerformanceMinorElection_idx" ON public."Voter" USING btree ("VotingPerformanceMinorElection");


--
-- Name: Voter_Wastewater_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Wastewater_District_idx" ON public."Voter" USING btree ("Wastewater_District");


--
-- Name: Voter_Water_Agency_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Agency_SubDistrict_idx" ON public."Voter" USING btree ("Water_Agency_SubDistrict");


--
-- Name: Voter_Water_Agency_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Agency_idx" ON public."Voter" USING btree ("Water_Agency");


--
-- Name: Voter_Water_Conservation_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Conservation_District_idx" ON public."Voter" USING btree ("Water_Conservation_District");


--
-- Name: Voter_Water_Conservation_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Conservation_SubDistrict_idx" ON public."Voter" USING btree ("Water_Conservation_SubDistrict");


--
-- Name: Voter_Water_Control_Water_Conservation_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Control_Water_Conservation_SubDistrict_idx" ON public."Voter" USING btree ("Water_Control_Water_Conservation_SubDistrict");


--
-- Name: Voter_Water_Control_Water_Conservation_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Control_Water_Conservation_idx" ON public."Voter" USING btree ("Water_Control_Water_Conservation");


--
-- Name: Voter_Water_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_District_idx" ON public."Voter" USING btree ("Water_District");


--
-- Name: Voter_Water_Public_Utility_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Public_Utility_District_idx" ON public."Voter" USING btree ("Water_Public_Utility_District");


--
-- Name: Voter_Water_Public_Utility_Subdistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Public_Utility_Subdistrict_idx" ON public."Voter" USING btree ("Water_Public_Utility_Subdistrict");


--
-- Name: Voter_Water_Replacement_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Replacement_District_idx" ON public."Voter" USING btree ("Water_Replacement_District");


--
-- Name: Voter_Water_Replacement_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_Replacement_SubDistrict_idx" ON public."Voter" USING btree ("Water_Replacement_SubDistrict");


--
-- Name: Voter_Water_SubDistrict_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Water_SubDistrict_idx" ON public."Voter" USING btree ("Water_SubDistrict");


--
-- Name: Voter_Weed_District_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "Voter_Weed_District_idx" ON public."Voter" USING btree ("Weed_District");


--
-- PostgreSQL database dump complete
--
