"""Canonical target-schema column list for the new voter DB.

**This is the single source of truth** for the columns the loader creates in
each `public."Voter{STATE}"` table — and therefore the ordered column list
the unload step writes, and the ordered column list that aws_s3 COPY
expects.

Scoped per PLAN_LOADER.md § "Column Scoping":
- Floor: the legacy 348 columns from the old dbt Python model's
  `VOTER_COLUMN_LIST` (in `gp-data-platform/dbt/project/models/write/
  write__l2_databricks_to_gp_api.py`).
- Minus: the three legacy typo'd identifiers, which are replaced by their
  L2-native corrected names.
- Plus: ~15 L2-native district columns that close
  `org_districts.l2Type`-lookup bugs (e.g. `Judicial_Justice_of_the_Peace`
  for Bell County TX).
- Plus: ~13 demographic/phone columns referenced by the people-api
  (`Voter.prisma`, `filters.sql.utils.ts`, `people.select.ts`).
- Plus: 2 derived integer sidecars (`Age_Int`, `Estimated_Income_Amount_Int`)
  computed during unload.
- Explicitly excluded: the ~300 ConsumerData_* interest columns,
  FECDonors_*, PRI_BLT_*, and all Databricks loader-internal columns
  (`SEQUENCE`, `loaded_at`, `state_postal_code`, `state_from_lalvoterid`).

Shape per entry: `TargetColumn(name, pg_type, source, spark_cast, kind)`.

- `name`: the final PG column name (mixed-case, will be double-quoted).
- `pg_type`: default PG type; step 3's emit_ddl will prefer the prod-dump
  type when one exists (that way we don't accidentally drift column types
  for existing columns).
- `source`: the Databricks source column this row comes from. Equals
  `name` for the vast majority. For the three fixed renames, this is the
  L2-native corrected name (which equals `name` in the new schema).
- `spark_cast`: optional Spark cast expression. `int` for the 6 legacy
  integer columns, `date` for the 2 legacy date columns, None for most.
- `kind`:
  - `"source"`: pulled from Databricks (the common case).
  - `"removed"`: no longer in the Databricks source — write NULL during
    unload, keep as NULL column in PG so column order stays stable.
  - `"derived"`: computed in Spark from another source column during unload
    (`Age_Int`, `Estimated_Income_Amount_Int`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ColumnKind = Literal["source", "removed", "derived"]


@dataclass(frozen=True, slots=True)
class TargetColumn:
    name: str
    pg_type: str
    source: str | None = None  # Databricks column; defaults to name when kind="source"
    spark_cast: str | None = None
    kind: ColumnKind = "source"
    derive_expr: str | None = None  # populated for derived columns


# --- The legacy 348 column names (from write__l2_databricks_to_gp_api.py) ---
# Verbatim for auditability. We'll compose VOTER_TARGET_COLUMNS below by
# stripping the 3 typo'd names, applying the REMOVED mask, and appending new
# columns and derived sidecars.

LEGACY_COLUMN_LIST: list[str] = [
    "LALVOTERID",
    "Voters_Active",
    "Voters_StateVoterID",
    "Voters_CountyVoterID",
    "VoterTelephones_LandlineFormatted",
    "VoterTelephones_LandlineConfidenceCode",
    "VoterTelephones_CellPhoneFormatted",
    "VoterTelephones_CellConfidenceCode",
    "Voters_FirstName",
    "Voters_MiddleName",
    "Voters_LastName",
    "Voters_NameSuffix",
    "Residence_Addresses_AddressLine",
    "Residence_Addresses_ExtraAddressLine",
    "Residence_Addresses_City",
    "Residence_Addresses_State",
    "Residence_Addresses_Zip",
    "Residence_Addresses_ZipPlus4",
    "Residence_Addresses_DPBC",
    "Residence_Addresses_CheckDigit",
    "Residence_Addresses_HouseNumber",
    "Residence_Addresses_PrefixDirection",
    "Residence_Addresses_StreetName",
    "Residence_Addresses_Designator",
    "Residence_Addresses_SuffixDirection",
    "Residence_Addresses_ApartmentNum",
    "Residence_Addresses_ApartmentType",
    "Residence_Addresses_CassErrStatCode",
    "Voters_SequenceZigZag",
    "Voters_SequenceOddEven",
    "Residence_Addresses_Latitude",
    "Residence_Addresses_Longitude",
    "Residence_Addresses_GeoHash",
    "Residence_Addresses_LatLongAccuracy",
    "Residence_HHParties_Description",
    "Mailing_Addresses_AddressLine",
    "Mailing_Addresses_ExtraAddressLine",
    "Mailing_Addresses_City",
    "Mailing_Addresses_State",
    "Mailing_Addresses_Zip",
    "Mailing_Addresses_ZipPlus4",
    "Mailing_Addresses_DPBC",
    "Mailing_Addresses_CheckDigit",
    "Mailing_Addresses_HouseNumber",
    "Mailing_Addresses_PrefixDirection",
    "Mailing_Addresses_StreetName",
    "Mailing_Addresses_Designator",
    "Mailing_Addresses_SuffixDirection",
    "Mailing_Addresses_ApartmentNum",
    "Mailing_Addresses_ApartmentType",
    "Mailing_Addresses_CassErrStatCode",
    "Mailing_Families_FamilyID",
    "Mailing_Families_HHCount",
    "Mailing_HHGender_Description",
    "Mailing_HHParties_Description",
    "Voters_Age",
    "Voters_Gender",
    "DateConfidence_Description",
    "Parties_Description",
    "VoterParties_Change_Changed_Party",
    "Ethnic_Description",
    "EthnicGroups_EthnicGroup1Desc",
    "CountyEthnic_LALEthnicCode",
    "CountyEthnic_Description",
    "Religions_Description",
    "Voters_CalculatedRegDate",
    "Voters_OfficialRegDate",
    "Voters_PlaceOfBirth",
    "Languages_Description",
    "AbsenteeTypes_Description",
    "MilitaryStatus_Description",
    "MaritalStatus_Description",
    "Voters_MovedFrom_State",
    "Voters_MovedFrom_Date",
    "Voters_MovedFrom_Party_Description",
    "Voters_VotingPerformanceEvenYearGeneral",
    "Voters_VotingPerformanceEvenYearPrimary",
    "Voters_VotingPerformanceEvenYearGeneralAndPrimary",
    "Voters_VotingPerformanceMinorElection",
    "General_2026",
    "Primary_2026",
    "OtherElection_2026",
    "AnyElection_2025",
    "General_2024",
    "Primary_2024",
    "PresidentialPrimary_2024",
    "OtherElection_2024",
    "AnyElection_2023",
    "General_2022",
    "Primary_2022",
    "OtherElection_2022",
    "AnyElection_2021",
    "General_2020",
    "Primary_2020",
    "PresidentialPrimary_2020",
    "OtherElection_2020",
    "AnyElection_2019",
    "General_2018",
    "Primary_2018",
    "OtherElection_2018",
    "AnyElection_2017",
    "General_2016",
    "Primary_2016",
    "PresidentialPrimary_2016",
    "OtherElection_2016",
    "US_Congressional_District",
    "AddressDistricts_Change_Changed_CD",
    "State_Senate_District",
    "AddressDistricts_Change_Changed_SD",
    "State_House_District",
    "AddressDistricts_Change_Changed_HD",
    "State_Legislative_District",
    "AddressDistricts_Change_Changed_LD",
    "County",
    "Voters_FIPS",
    "AddressDistricts_Change_Changed_County",
    "Precinct",
    "County_Legislative_District",
    "City",
    "City_Council_Commissioner_District",
    "County_Commissioner_District",
    "County_Supervisorial_District",
    "City_Mayoral_District",
    "Town_District",
    "Town_Council",
    "Village",
    "Township",
    "Borough",
    "Hamlet_Community_Area",
    "City_Ward",
    "Town_Ward",
    "Township_Ward",
    "Village_Ward",
    "Borough_Ward",
    "Board_of_Education_District",
    "Board_of_Education_SubDistrict",
    "City_School_District",
    "College_Board_District",
    "Community_College_Commissioner_District",
    "Community_College_SubDistrict",
    "County_Board_of_Education_District",
    "County_Board_of_Education_SubDistrict",
    "County_Community_College_District",
    "County_Superintendent_of_Schools_District",
    "County_Unified_School_District",
    "District_Attorney",
    "Education_Commission_District",
    "Educational_Service_District",
    "Election_Commissioner_District",
    "Elementary_School_District",
    "Elementary_School_SubDistrict",
    "Exempted_Village_School_District",
    "High_School_District",
    "High_School_SubDistrict",
    "Judicial_Appellate_District",
    "Judicial_Circuit_Court_District",
    "Judicial_County_Board_of_Review_District",
    "Judicial_County_Court_District",
    "Judicial_District",
    "Judicial_District_Court_District",
    "Judicial_Family_Court_District",
    "Judicial_Jury_District",
    "Judicial_Juvenile_Court_District",
    "Judicial_Magistrate_Division",
    "Judicial_Sub_Circuit_District",
    "Judicial_Superior_Court_District",
    "Judicial_Supreme_Court_District",
    "Middle_School_District",
    "Municipal_Court_District",
    "Proposed_City_Commissioner_District",
    "Proposed_Elementary_School_District",
    "Proposed_Unified_School_District",
    "Regional_Office_of_Education_District",
    "School_Board_District",
    "School_District",
    "School_District_Vocational",
    "School_Facilities_Improvement_District",
    "School_Subdistrict",
    "Service_Area_District",
    "Superintendent_of_Schools_District",
    "Unified_School_District",
    "Unified_School_SubDistrict",
    "Coast_Water_District",
    "Consolidated_Water_District",
    "County_Water_District",
    "County_Water_Landowner_District",
    "County_Water_SubDistrict",
    "Metropolitan_Water_District",
    "Mountain_Water_District",
    "Municipal_Water_District",
    "Municipal_Water_SubDistrict",
    "River_Water_District",
    "Water_Agency",
    "Water_Agency_SubDistrict",
    "Water_Conservation_District",
    "Water_Conservation_SubDistrict",
    "Water_Control__Water_Conservation",
    "Water_Control__Water_Conservation_SubDistrict",
    "Water_District",
    "Water_Public_Utility_District",
    "Water_Public_Utility_Subdistrict",
    "Water_Replacement_District",
    "Water_Replacement_SubDistrict",
    "Water_SubDistrict",
    "County_Fire_District",
    "Fire_District",
    "Fire_Maintenance_District",
    "Fire_Protection_District",
    "Fire_Protection_SubDistrict",
    "Fire_Protection_Tax_Measure_District",
    "Fire_Service_Area_District",
    "Fire_SubDistrict",
    "Independent_Fire_District",
    "Proposed_Fire_District",
    "Unprotected_Fire_District",
    "Bay_Area_Rapid_Transit",
    "Metro_Transit_District",
    "Rapid_Transit_District",
    "Rapid_Transit_SubDistrict",
    "Transit_District",
    "Transit_SubDistrict",
    "Community_Service_District",
    "Community_Service_SubDistrict",
    "County_Service_Area",
    "County_Service_Area_SubDistrict",
    "TriCity_Service_District",
    "Library_Services_District",
    "Airport_District",
    "Annexation_District",
    "Aquatic_Center_District",
    "Aquatic_District",
    "Assessment_District",
    "Bonds_District",
    "Career_Center",
    "Cemetery_District",
    "Central_Committee_District",
    "Chemical_Control_District",
    "Committee_Super_District",
    "Communications_District",
    "Community_College_At_Large",
    "Community_Council_District",
    "Community_Council_SubDistrict",
    "Community_Facilities_District",
    "Community_Facilities_SubDistrict",
    "Community_Hospital_District",
    "Community_Planning_Area",
    "Congressional_Township",
    "Conservation_District",
    "Conservation_SubDistrict",
    "Control_Zone_District",
    "Corrections_District",
    "County_Hospital_District",
    "County_Library_District",
    "County_Memorial_District",
    "County_Paramedic_District",
    "County_Sewer_District",
    "Democratic_Convention_Member",
    "Democratic_Zone",
    "Designated_Market_Area_DMA",
    "Drainage_District",
    "Educational_Service_Subdistrict",
    "Emergency_Communication_911_District",
    "Emergency_Communication_911_SubDistrict",
    "Enterprise_Zone_District",
    "EXT_District",
    "Facilities_Improvement_District",
    "Flood_Control_Zone",
    "Forest_Preserve",
    "Garbage_District",
    "Geological_Hazard_Abatement_District",
    "Health_District",
    "Hospital_SubDistrict",
    "Improvement_Landowner_District",
    "Irrigation_District",
    "Irrigation_SubDistrict",
    "Island",
    "Land_Commission",
    "Landscaping_And_Lighting_Assessment_Distric",
    "Law_Enforcement_District",
    "Learning_Community_Coordinating_Council_District",
    "Levee_District",
    "Levee_Reconstruction_Assesment_District",
    "Library_District",
    "Library_SubDistrict",
    "Lighting_District",
    "Local_Hospital_District",
    "Local_Park_District",
    "Maintenance_District",
    "Master_Plan_District",
    "Memorial_District",
    "Metro_Service_District",
    "Metro_Service_Subdistrict",
    "Mosquito_Abatement_District",
    "Multi_township_Assessor",
    "Municipal_Advisory_Council_District",
    "Municipal_Utility_District",
    "Municipal_Utility_SubDistrict",
    "Museum_District",
    "Northeast_Soil_and_Water_District",
    "Open_Space_District",
    "Open_Space_SubDistrict",
    "Other",
    "Paramedic_District",
    "Park_Commissioner_District",
    "Park_District",
    "Park_SubDistrict",
    "Planning_Area_District",
    "Police_District",
    "Port_District",
    "Port_SubDistrict",
    "Power_District",
    "Proposed_City",
    "Proposed_Community_College",
    "Proposed_District",
    "Public_Airport_District",
    "Public_Regulation_Commission",
    "Public_Service_Commission_District",
    "Public_Utility_District",
    "Public_Utility_SubDistrict",
    "Reclamation_District",
    "Recreation_District",
    "Recreational_SubDistrict",
    "Republican_Area",
    "Republican_Convention_Member",
    "Resort_Improvement_District",
    "Resource_Conservation_District",
    "Road_Maintenance_District",
    "Rural_Service_District",
    "Sanitary_District",
    "Sanitary_SubDistrict",
    "Sewer_District",
    "Sewer_Maintenance_District",
    "Sewer_SubDistrict",
    "Snow_Removal_District",
    "Soil_And_Water_District",
    "Soil_And_Water_District_At_Large",
    "Special_Reporting_District",
    "Special_Tax_District",
    "Storm_Water_District",
    "Street_Lighting_District",
    "TV_Translator_District",
    "Unincorporated_District",
    "Unincorporated_Park_District",
    "Ute_Creek_Soil_District",
    "Vector_Control_District",
    "Vote_By_Mail_Area",
    "Wastewater_District",
    "Weed_District",
]

# Columns L2 stopped delivering but the legacy PG schema still holds. The
# new loader keeps them as NULL placeholders to keep column order stable
# for any consumer that still references them. (This list is exactly the
# REMOVED_COLUMNS from the old loader, minus the two typo'd names that
# the rename table retires.) `Mailing_HHGender_Description` is here —
# an earlier draft had it as "restored from L2," but the Databricks
# source only has `Residence_HHGender_Description` (residence vs mailing
# are different concepts), so it stays a NULL placeholder.
REMOVED_COLUMNS: frozenset[str] = frozenset(
    {
        "Residence_Addresses_GeoHash",
        "Mailing_Families_HHCount",
        "Mailing_HHGender_Description",
        "Mailing_HHParties_Description",
        "DateConfidence_Description",
        "Religions_Description",
        "Languages_Description",
        "MilitaryStatus_Description",
        "MaritalStatus_Description",
        "Municipal_Court_District",
        "Soil_And_Water_District",
        "Soil_And_Water_District_At_Large",
    }
)

# The 6 integer columns that must be cast during unload — the source is
# STRING in Databricks but the PG column is INTEGER. Using try_cast in
# Spark turns unparseable values into NULL instead of failing the job.
INTEGER_COLUMNS: frozenset[str] = frozenset(
    {
        "Residence_Addresses_CheckDigit",
        "Residence_Addresses_PrefixDirection",
        "Residence_Addresses_SuffixDirection",
        "Mailing_Addresses_CheckDigit",
        "Mailing_Addresses_PrefixDirection",
        "Mailing_Addresses_SuffixDirection",
    }
)

# The 2 date columns cast during unload.
DATE_COLUMNS: frozenset[str] = frozenset(
    {
        "Voters_CalculatedRegDate",
        "Voters_MovedFrom_Date",
    }
)

# The boolean election columns — PG type BOOLEAN, Spark source is boolean.
BOOLEAN_COLUMNS: frozenset[str] = frozenset(
    {
        "Voters_Active",
        "General_2026",
        "Primary_2026",
        "OtherElection_2026",
        "AnyElection_2025",
        "General_2024",
        "Primary_2024",
        "PresidentialPrimary_2024",
        "OtherElection_2024",
        "AnyElection_2023",
        "General_2022",
        "Primary_2022",
        "OtherElection_2022",
        "AnyElection_2021",
        "General_2020",
        "Primary_2020",
        "PresidentialPrimary_2020",
        "OtherElection_2020",
        "AnyElection_2019",
        "General_2018",
        "Primary_2018",
        "OtherElection_2018",
        "AnyElection_2017",
        "General_2016",
        "Primary_2016",
        "PresidentialPrimary_2016",
        "OtherElection_2016",
    }
)

# Three typo'd legacy names to retire. Each maps to the L2-native
# corrected name (already present in the Databricks source under that
# name). The NEW cluster's schema has only the corrected name.
LEGACY_RENAMES: dict[str, str] = {
    "Water_Control__Water_Conservation": "Water_Control_Water_Conservation",
    "Water_Control__Water_Conservation_SubDistrict": "Water_Control_Water_Conservation_SubDistrict",
    "Landscaping_And_Lighting_Assessment_Distric": "Landscaping_and_Lighting_Assessment_District",
}

# New L2-native district columns — adding these closes the class of
# "Error counting Records" bugs that occur when `org_districts.l2Type`
# names a column the PG schema doesn't have (e.g. Bell County TX's
# `Judicial_Justice_of_the_Peace`).
NEW_DISTRICT_COLUMNS: list[str] = [
    "Judicial_Chancery_Court",
    "Judicial_Justice_of_the_Peace",
    "Judicial_Municipal_Court_District",
    "Community_College",
    "Hospital_District",
    "State_Board_of_Equalization",
    "4H_Livestock_District",
    "2024_Proposed_Congressional_District",
    "2024_Proposed_State_Senate_District",
    "2024_Proposed_State_House_District",
    "2024_Proposed_State_Legislative_District",
    "2001_US_Congressional_District",
    "2001_State_Senate_District",
    "2001_State_House_District",
    "2001_State_Legislative_District",
    "2010_US_Congressional_District",
    "2010_State_Senate_District",
    "2010_State_House_District",
    "2010_State_Legislative_District",
]

# New demographic / phone-availability columns referenced by the
# people-api (Voter.prisma + filters.sql.utils.ts).
#
# Each tuple: (name, pg_type). Source column == name.
NEW_DEMOGRAPHIC_COLUMNS: list[tuple[str, str]] = [
    ("Voters_BirthDate", "TEXT"),
    ("BirthDateConfidence_Description", "TEXT"),
    ("ConsumerData_Business_Owner", "TEXT"),
    ("ConsumerData_Education_of_Person", "TEXT"),
    ("ConsumerData_Estimated_Income_Amount", "TEXT"),
    ("ConsumerData_Homeowner_Probability_Model", "TEXT"),
    ("ConsumerData_Language_Code", "TEXT"),
    ("ConsumerData_Marital_Status", "TEXT"),
    ("ConsumerData_Presence_Of_Children_in_HH", "TEXT"),
    ("ConsumerDataLL_Veteran", "TEXT"),
    ("Residence_Families_FamilyID", "TEXT"),
    ("Phone_Number_Available", "TEXT"),
    ("Cell_Phone_Number_Available", "TEXT"),
    ("Landline_Phone_Number_Available", "TEXT"),
]

# Derived columns — computed during unload from other source columns
# via `try_cast`. These don't exist in the Databricks source.
DERIVED_COLUMNS: list[TargetColumn] = [
    TargetColumn(
        name="Age_Int",
        pg_type="INTEGER",
        source=None,
        kind="derived",
        derive_expr="try_cast(Voters_Age AS INT)",
    ),
    TargetColumn(
        name="Estimated_Income_Amount_Int",
        pg_type="INTEGER",
        source=None,
        kind="derived",
        derive_expr="try_cast(ConsumerData_Estimated_Income_Amount AS INT)",
    ),
]


def _pg_type_for(name: str) -> str:
    if name in BOOLEAN_COLUMNS:
        return "BOOLEAN"
    if name in INTEGER_COLUMNS:
        return "INTEGER"
    if name in DATE_COLUMNS:
        return "DATE"
    return "TEXT"


def _spark_cast_for(name: str) -> str | None:
    if name in INTEGER_COLUMNS:
        return "int"
    if name in DATE_COLUMNS:
        return "date"
    return None


def _build_target_columns() -> list[TargetColumn]:
    out: list[TargetColumn] = []
    seen: set[str] = set()

    # 1. Legacy 348, with renames applied in place and REMOVED_COLUMNS
    #    flagged as kind="removed".
    for legacy in LEGACY_COLUMN_LIST:
        target_name = LEGACY_RENAMES.get(legacy, legacy)
        if target_name in seen:
            continue
        seen.add(target_name)

        if target_name in LEGACY_RENAMES.values():
            # A renamed column: its data IS in L2 under the corrected name.
            # So kind=source, source=target_name.
            out.append(
                TargetColumn(
                    name=target_name,
                    pg_type=_pg_type_for(target_name),
                    source=target_name,
                    spark_cast=_spark_cast_for(target_name),
                    kind="source",
                )
            )
            continue

        if legacy in REMOVED_COLUMNS:
            out.append(
                TargetColumn(
                    name=target_name,
                    pg_type=_pg_type_for(target_name),
                    source=None,
                    spark_cast=None,
                    kind="removed",
                )
            )
            continue

        out.append(
            TargetColumn(
                name=target_name,
                pg_type=_pg_type_for(target_name),
                source=target_name,
                spark_cast=_spark_cast_for(target_name),
                kind="source",
            )
        )

    # 2. New district columns (plain source columns, TEXT).
    for name in NEW_DISTRICT_COLUMNS:
        if name in seen:
            continue
        seen.add(name)
        out.append(TargetColumn(name=name, pg_type="TEXT", source=name, kind="source"))

    # 3. New demographic columns.
    for name, pg_type in NEW_DEMOGRAPHIC_COLUMNS:
        if name in seen:
            continue
        seen.add(name)
        out.append(TargetColumn(name=name, pg_type=pg_type, source=name, kind="source"))

    # 4. Derived sidecars (appended last — column order after all
    #    source-driven columns).
    for col in DERIVED_COLUMNS:
        if col.name in seen:
            continue
        seen.add(col.name)
        out.append(col)

    return out


VOTER_TARGET_COLUMNS: list[TargetColumn] = _build_target_columns()


def columns_by_name() -> dict[str, TargetColumn]:
    return {c.name: c for c in VOTER_TARGET_COLUMNS}


def source_columns() -> list[TargetColumn]:
    """Columns that read directly from a Databricks source column."""
    return [c for c in VOTER_TARGET_COLUMNS if c.kind == "source"]


def removed_columns() -> list[TargetColumn]:
    return [c for c in VOTER_TARGET_COLUMNS if c.kind == "removed"]


def derived_columns() -> list[TargetColumn]:
    return [c for c in VOTER_TARGET_COLUMNS if c.kind == "derived"]
