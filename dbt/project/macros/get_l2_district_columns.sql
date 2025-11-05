{% macro get_l2_district_columns(use_backticks=true, cast_to_string=false) %}
    {#-
    Returns a comma-separated list of L2 district columns for use in SELECT statements or UNPIVOT clauses.
    This macro provides all district-related columns from the L2 nationwide uniform dataset.

    Args:
        use_backticks (bool): Whether to wrap column names in backticks. Defaults to true for SELECT statements.
                             Set to false for UNPIVOT clauses.
        cast_to_string (bool): Whether to cast columns to STRING. Useful for UNPIVOT when columns have mixed types.
                              Defaults to false.

    Usage:
        SELECT
            {{ get_l2_district_columns() }}
        FROM {{ ref('some_model') }}

        SELECT
            {{ get_l2_district_columns(cast_to_string=true) }}
        FROM {{ ref('some_model') }}

        UNPIVOT (
            value FOR column_name IN (
                {{ get_l2_district_columns(use_backticks=false) }}
            )
        )

    Returns:
        string: Comma-separated list of district columns with proper formatting
    -#}
    {%- set district_columns = [
        "`AddressDistricts_Change_Changed_CD`",
        "`AddressDistricts_Change_Changed_County`",
        "`AddressDistricts_Change_Changed_HD`",
        "`AddressDistricts_Change_Changed_LD`",
        "`AddressDistricts_Change_Changed_SD`",
        "`Airport_District`",
        "`Annexation_District`",
        "`Aquatic_Center_District`",
        "`Aquatic_District`",
        "`Assessment_District`",
        "`Bay_Area_Rapid_Transit`",
        "`Board_of_Education_District`",
        "`Board_of_Education_SubDistrict`",
        "`Bonds_District`",
        "`Borough`",
        "`Borough_Ward`",
        "`Career_Center`",
        "`Cemetery_District`",
        "`Central_Committee_District`",
        "`Chemical_Control_District`",
        "`Committee_Super_District`",
        "`City`",
        "`City_Council_Commissioner_District`",
        "`City_Mayoral_District`",
        "`City_School_District`",
        "`City_Ward`",
        "`Coast_Water_District`",
        "`College_Board_District`",
        "`Communications_District`",
        "`Community_College_At_Large`",
        "`Community_College_Commissioner_District`",
        "`Community_College_SubDistrict`",
        "`Community_Council_District`",
        "`Community_Council_SubDistrict`",
        "`Community_Facilities_District`",
        "`Community_Facilities_SubDistrict`",
        "`Community_Hospital_District`",
        "`Community_Planning_Area`",
        "`Community_Service_District`",
        "`Community_Service_SubDistrict`",
        "`Congressional_Township`",
        "`Conservation_District`",
        "`Conservation_SubDistrict`",
        "`Consolidated_Water_District`",
        "`Control_Zone_District`",
        "`Corrections_District`",
        "`County`",
        "`County_Board_of_Education_District`",
        "`County_Board_of_Education_SubDistrict`",
        "`County_Community_College_District`",
        "`County_Commissioner_District`",
        "`County_Fire_District`",
        "`County_Hospital_District`",
        "`County_Legislative_District`",
        "`County_Library_District`",
        "`County_Memorial_District`",
        "`County_Paramedic_District`",
        "`County_Service_Area`",
        "`County_Service_Area_SubDistrict`",
        "`County_Sewer_District`",
        "`County_Superintendent_of_Schools_District`",
        "`County_Supervisorial_District`",
        "`County_Unified_School_District`",
        "`County_Water_District`",
        "`County_Water_Landowner_District`",
        "`County_Water_SubDistrict`",
        "`Democratic_Convention_Member`",
        "`Democratic_Zone`",
        "`Designated_Market_Area_DMA`",
        "`District_Attorney`",
        "`Drainage_District`",
        "`Education_Commission_District`",
        "`Educational_Service_District`",
        "`Educational_Service_Subdistrict`",
        "`Election_Commissioner_District`",
        "`Elementary_School_District`",
        "`Elementary_School_SubDistrict`",
        "`Emergency_Communication_911_District`",
        "`Emergency_Communication_911_SubDistrict`",
        "`Enterprise_Zone_District`",
        "`EXT_District`",
        "`Exempted_Village_School_District`",
        "`Facilities_Improvement_District`",
        "`Voters_FIPS`",
        "`Fire_District`",
        "`Fire_Maintenance_District`",
        "`Fire_Protection_District`",
        "`Fire_Protection_SubDistrict`",
        "`Fire_Protection_Tax_Measure_District`",
        "`Fire_Service_Area_District`",
        "`Fire_SubDistrict`",
        "`Flood_Control_Zone`",
        "`Forest_Preserve`",
        "`Garbage_District`",
        "`Geological_Hazard_Abatement_District`",
        "`Hamlet_Community_Area`",
        "`Health_District`",
        "`High_School_District`",
        "`High_School_SubDistrict`",
        "`Hospital_SubDistrict`",
        "`Improvement_Landowner_District`",
        "`Independent_Fire_District`",
        "`Irrigation_District`",
        "`Irrigation_SubDistrict`",
        "`Island`",
        "`Judicial_Appellate_District`",
        "`Judicial_Circuit_Court_District`",
        "`Judicial_County_Board_of_Review_District`",
        "`Judicial_County_Court_District`",
        "`Judicial_District`",
        "`Judicial_District_Court_District`",
        "`Judicial_Family_Court_District`",
        "`Judicial_Jury_District`",
        "`Judicial_Juvenile_Court_District`",
        "`Judicial_Magistrate_Division`",
        "`Judicial_Municipal_Court_District`",
        "`Judicial_Sub_Circuit_District`",
        "`Judicial_Superior_Court_District`",
        "`Judicial_Supreme_Court_District`",
        "`Landscaping_and_Lighting_Assessment_District`",
        "`Land_Commission`",
        "`Law_Enforcement_District`",
        "`Learning_Community_Coordinating_Council_District`",
        "`Levee_District`",
        "`Levee_Reconstruction_Assesment_District`",
        "`Library_District`",
        "`Library_Services_District`",
        "`Library_SubDistrict`",
        "`Lighting_District`",
        "`Local_Hospital_District`",
        "`Local_Park_District`",
        "`Maintenance_District`",
        "`Master_Plan_District`",
        "`Memorial_District`",
        "`Metro_Service_District`",
        "`Metro_Service_Subdistrict`",
        "`Metro_Transit_District`",
        "`Metropolitan_Water_District`",
        "`Middle_School_District`",
        "`Mosquito_Abatement_District`",
        "`Mountain_Water_District`",
        "`Multi_township_Assessor`",
        "`Municipal_Advisory_Council_District`",
        "`Municipal_Utility_District`",
        "`Municipal_Utility_SubDistrict`",
        "`Municipal_Water_District`",
        "`Municipal_Water_SubDistrict`",
        "`Museum_District`",
        "`Northeast_Soil_and_Water_District`",
        "`Open_Space_District`",
        "`Open_Space_SubDistrict`",
        "`Other`",
        "`Paramedic_District`",
        "`Park_Commissioner_District`",
        "`Park_District`",
        "`Park_SubDistrict`",
        "`Planning_Area_District`",
        "`Police_District`",
        "`Port_District`",
        "`Port_SubDistrict`",
        "`Power_District`",
        "`Precinct`",
        "`Proposed_City`",
        "`Proposed_City_Commissioner_District`",
        "`Proposed_Community_College`",
        "`Proposed_District`",
        "`Proposed_Elementary_School_District`",
        "`Proposed_Fire_District`",
        "`Proposed_Unified_School_District`",
        "`Public_Airport_District`",
        "`Public_Regulation_Commission`",
        "`Public_Service_Commission_District`",
        "`Public_Utility_District`",
        "`Public_Utility_SubDistrict`",
        "`Rapid_Transit_District`",
        "`Rapid_Transit_SubDistrict`",
        "`Reclamation_District`",
        "`Recreation_District`",
        "`Recreational_SubDistrict`",
        "`Regional_Office_of_Education_District`",
        "`Republican_Area`",
        "`Republican_Convention_Member`",
        "`Resort_Improvement_District`",
        "`Resource_Conservation_District`",
        "`River_Water_District`",
        "`Road_Maintenance_District`",
        "`Rural_Service_District`",
        "`Sanitary_District`",
        "`Sanitary_SubDistrict`",
        "`School_Board_District`",
        "`School_District`",
        "`School_District_Vocational`",
        "`School_Facilities_Improvement_District`",
        "`School_Subdistrict`",
        "`Service_Area_District`",
        "`Sewer_District`",
        "`Sewer_Maintenance_District`",
        "`Sewer_SubDistrict`",
        "`Snow_Removal_District`",
        "`Special_Reporting_District`",
        "`Special_Tax_District`",
        "`State_House_District`",
        "`State_Legislative_District`",
        "`State_Senate_District`",
        "`Storm_Water_District`",
        "`Street_Lighting_District`",
        "`Superintendent_of_Schools_District`",
        "`TV_Translator_District`",
        "`Town_Council`",
        "`Town_District`",
        "`Town_Ward`",
        "`Township`",
        "`Township_Ward`",
        "`Transit_District`",
        "`Transit_SubDistrict`",
        "`TriCity_Service_District`",
        "`US_Congressional_District`",
        "`Unified_School_District`",
        "`Unified_School_SubDistrict`",
        "`Unincorporated_District`",
        "`Unincorporated_Park_District`",
        "`Unprotected_Fire_District`",
        "`Ute_Creek_Soil_District`",
        "`Vector_Control_District`",
        "`Village`",
        "`Village_Ward`",
        "`Vote_By_Mail_Area`",
        "`Wastewater_District`",
        "`Water_Agency`",
        "`Water_Agency_SubDistrict`",
        "`Water_Conservation_District`",
        "`Water_Conservation_SubDistrict`",
        "`Water_Control_Water_Conservation`",
        "`Water_Control_Water_Conservation_SubDistrict`",
        "`Water_District`",
        "`Water_Public_Utility_District`",
        "`Water_Public_Utility_Subdistrict`",
        "`Water_Replacement_District`",
        "`Water_Replacement_SubDistrict`",
        "`Water_SubDistrict`",
        "`Weed_District`",
    ] -%}

    {%- if use_backticks -%}
        {#- For SELECT statements -#}
        {%- if cast_to_string -%}
            {#- Cast all columns to STRING for UNPIVOT compatibility -#}
            {%- set column_expressions = [] -%}
            {%- for col in district_columns -%}
                {%- set col_name = col | replace("`", "") | trim -%}
                {%- set base_col = col_name -%}
                {%- set output_col = col_name -%}
                {%- if " as " in col_name -%}
                    {%- set base_col = col_name.split(" as ")[0] | trim -%}
                    {%- set output_col = (
                        col_name.split(" as ")[1] | trim | replace("`", "")
                    ) -%}
                {%- endif -%}
                {%- set cast_expr = (
                    "cast(`"
                    ~ base_col
                    ~ "` as string) as `"
                    ~ output_col
                    ~ "`"
                ) -%}
                {%- set _ = column_expressions.append(cast_expr) -%}
            {%- endfor -%}
            {{ column_expressions | join(",\n            ") }}
        {%- else -%}
            {#- Return columns as-is with aliases -#}
            {{ district_columns | join(",\n            ") }}
        {%- endif -%}
    {%- else -%}
        {#- For UNPIVOT, extract column names without backticks and handle aliases -#}
        {%- set column_names = [] -%}
        {%- for col in district_columns -%}
            {%- set col_name = col | replace("`", "") | trim -%}
            {%- if " as " in col_name -%}
                {%- set col_name = col_name.split(" as ")[0] | trim -%}
            {%- endif -%}
            {%- set _ = column_names.append(col_name) -%}
        {%- endfor -%}
        {{ column_names | join(",\n            ") }}
    {%- endif -%}
{% endmacro %}
