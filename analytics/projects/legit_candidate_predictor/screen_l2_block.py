"""DATA-2239 L2/haystaq FULL-BLOCK screen, step 1: mechanical triage (label-free).

Input: the cached catalog inventory (datasets/catalog_columns.parquet) for
int__l2_nationwide_uniform_w_haystaq (1,396 columns). Output:
datasets/l2_block_dispositions.csv — one row per column with a disposition and reason.

Dispositions (ordered rules; first match wins):
  drop_id_meta     identifiers, sequence/meta, load timestamps, join keys
  drop_pii         names, raw birth dates/places, addresses, phones, MAIDs, lat/lng
  drop_protected   gender / ethnicity / religion / language blocks (DECIDED 2026-08-13,
                   a priori, both model tiers) — includes HH gender composition and
                   religious-consumer flags; these columns are ALSO the proxy-screen targets
  drop_protected_pending  disability flags — same logic as the decided blocks, but not in
                   the decided list; RECOMMEND drop, surfaced as a pending decision
  drop_geo         district/ward/precinct/city/county strings, census geocodes, CBSA/MSA/
                   CSA/DMA — high-cardinality geography (model already carries state; fine
                   geography at 20k rows is memorization + coverage proxy, not signal)
  drop_freetext    employer names, auto make/model, FEC recipient, standardized titles
  drop_unknown     vendor-reserved columns with undocumented semantics
  keep             candidate for the proxy screen and the full-block experiment

TIME_DISCIPLINE per kept column (the L2 snapshot is acquired independently of candidate
arrival, so attributes are a-priori unless year/date-indexed):
  apriori      snapshot attribute, admissible as-is
  year_gated   year-indexed vote history (General_/Primary_/AnyElection_/OtherElection_/
               PresidentialPrimary_/PRI_BLT_): cell-masked to NaN when year >= signup year
  date_gated   registration dates, MovedFrom date, FEC dates/amounts: gated exactly as in
               build_l2_match.py (reg-after-signup NaN'd; FEC only when last donation
               predates signup)

Run:  cd analytics && uv run --with pyarrow python \
        projects/legit_candidate_predictor/screen_l2_block.py
"""

import re
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent

# ---------------------------------------------------------------- rule tables

ID_META = {
    "LALVOTERID",
    "SEQUENCE",
    "loaded_at",
    "haystaq_flags_loaded_at",
    "haystaq_scores_loaded_at",
    "state_from_lalvoterid",
    "state_postal_code",
    "Voters_FIPS",
    "Voters_CountyVoterID",
    "Voters_StateVoterID",
    "Voters_SequenceOddEven",
    "Voters_SequenceZigZag",
    "Residence_Families_FamilyID",
    "Mailing_Families_FamilyID",
    "BirthDateConfidence_Description",  # vendor QA meta, not a person attribute
    "VoterTelephones_CellConfidenceCode",  # vendor match-confidence meta
}

# The person-identifying columns. Voters_Age stays (l2_age accepted); the raw birth date
# and the name fields do not. Latitude/Longitude are point-of-residence identifiers; the
# distance FEATURE they could power needs a centroid source we do not have (catalog 4.4).
PII_EXACT = {
    "Voters_FirstName",
    "Voters_LastName",
    "Voters_MiddleName",
    "Voters_NameSuffix",
    "Voters_BirthDate",
    "Voters_PlaceOfBirth",
    "Residence_Addresses_Latitude",
    "Residence_Addresses_Longitude",
}
PII_PREFIX = (
    "Mailing_Addresses_",
    "ConsumerData_MAID",
    "VoterTelephones_CellPhone",
    "VoterTelephones_Landline",
)
# cell-phone-ONLY household is a flag (the catalog's phone-type feature), not a number
PII_EXCEPT = {"VoterTelephones_CellPhoneOnly"}
# residence address parts are PII; the two coded density/urbanicity fields are kept below
PII_RESIDENCE = {
    "Residence_Addresses_AddressLine",
    "Residence_Addresses_ApartmentNum",
    "Residence_Addresses_ApartmentType",
    "Residence_Addresses_CassErrStatCode",
    "Residence_Addresses_CheckDigit",
    "Residence_Addresses_City",
    "Residence_Addresses_DPBC",
    "Residence_Addresses_Designator",
    "Residence_Addresses_ExtraAddressLine",
    "Residence_Addresses_HouseNumber",
    "Residence_Addresses_PrefixDirection",
    "Residence_Addresses_StreetName",
    "Residence_Addresses_SuffixDirection",
    "Residence_Addresses_Zip",
    "Residence_Addresses_ZipPlus4",
    "Residence_Addresses_State",
    "Residence_Addresses_LatLongAccuracy",
    "Residence_Addresses_CensusBlock",
    "Residence_Addresses_CensusBlockGroup",
    "Residence_Addresses_CensusTract",
    "Residence_Addresses_Complete_Census_Geocode",
}

# DECIDED 2026-08-13: gender / ethnicity / religion / language, a priori, both tiers.
# These are also the proxy-screen TARGETS (pulled only in the screening sample).
PROTECTED = {
    # gender
    "Voters_Gender",
    "Residence_HHGender_Description",
    # ethnicity / national origin
    "EthnicGroups_EthnicGroup1Desc",
    "Ethnic_Description",
    "CountyEthnic_Description",
    "CountyEthnic_LALEthnicCode",
    "ConsumerData_Hispanic_Country_Code",
    "ConsumerData_Assimilation_Status",
    "ConsumerData_African_American_Professional_in_Household",
    # religion
    "ConsumerData_Religion_Code",
    "ConsumerData_Christian_Families",
    "ConsumerData_Religious_Contributor",
    "ConsumerData_Religious_Inspiration",
    "ConsumerData_Religious_Mags",
    "ConsumerData_Charity_Giving_Relig_Cause",
    # language
    "ConsumerData_Language_Code",
    "ConsumerData_AreaPcntHHSpanishSpeaking",
    # religion-adjacent haystaq: directly religiosity, not an issue position
    "hs_attends_church_frequently",
    "hs_attends_church_never",
    "hf_attends_church",
    "hs_religion_important",
    "hs_religion_not_important",
    "hf_religion",
    "hs_charity_giving_relig_cause",
}
# HH gender/age composition literally counts household members by gender
PROTECTED_PREFIX = (
    "ConsumerData_Females_in_HH",
    "ConsumerData_Males_in_HH",
    "ConsumerData_Unknown_Gender_in_HH",
)
PROTECTED_RE = re.compile(r"^ConsumerData_Children_in_HH_Age_\d+_\d+_(Female|Male|Unknown)$")

# Not in the decided list but the same logic applies — RECOMMEND drop, pending decision.
PROTECTED_PENDING = {
    "ConsumerData_Disabled_In_HH",
    "ConsumerData_AccessibilityHandicapFlag",
}

FREETEXT = {
    "ConsumerData_LikelyRecentEmployment_Company_Name",
    "ConsumerData_LikelyRecentEmployment_Standardized_Title",
    "ConsumerData_Auto_Make_1",
    "ConsumerData_Auto_Make_2",
    "ConsumerData_Auto_Model_1",
    "ConsumerData_Auto_Model_2",
    "ConsumerData_Motorcycle_Make_1",
    "ConsumerData_Motorcycle_Model_1",
    "FECDonors_PrimaryRecipientOfContributions",
}

UNKNOWN_SEMANTICS_RE = re.compile(r"^ConsumerData_Reserved_\d+$")

# geography kept: two coded urbanicity fields (low-cardinality, real signal class)
GEO_KEEP = {"ConsumerData_RUS_Code", "Residence_Addresses_Density"}
# geography dropped: named metro/market areas and their codes
GEO_EXACT = {
    "ConsumerData_CBSA",
    "ConsumerData_CBSA_Code",
    "ConsumerData_CSA",
    "ConsumerData_CSA_Code",
    "ConsumerData_MSA",
    "ConsumerData_MSA_Code",
    "Designated_Market_Area_DMA",
    "ConsumerData_Time_Zone",
    "Voters_MovedFrom_State",
}
# the ~450 district/jurisdiction strings: everything matching these patterns
GEO_RE = re.compile(
    r"(_District$|_SubDistrict$|_Subdistrict$|District_|^Judicial_|_Ward$|^Precinct$"
    r"|^City$|^County$|^Borough|^Township|^Village|^Island$|^Hamlet|^Town_"
    r"|^Congressional_Township$|^Multi_township_Assessor$|^Other$"
    r"|^\d{4}_(Proposed_)?(State|US)_|^2024_Proposed_"
    r"|School_District|Community_College|_Zone$|_Area$|_At_Large$"
    r"|^Water_|^Sewer_|^Fire_|^Library_|^Park_|^Hospital_|^Transit_|^Port_"
    r"|^Metro_|^Irrigation_|^Levee_|^Drainage_|^Cemetery_|^Airport_|^Sanitary_"
    r"|^Flood_|^Forest_|^Garbage_|^Health_|^Lighting_|^Maintenance_|^Memorial_"
    r"|^Mosquito_|^Museum_|^Paramedic_|^Police_|^Power_|^Reclamation_|^Recreation"
    r"|^Road_|^Rural_|^Snow_|^Soil_|^Storm_|^Street_|^Weed_|^Wastewater_"
    r"|^Corrections_|^Conservation_|^Annexation_|^Aquatic_|^Assessment_|^Bay_Area"
    r"|^Bonds_|^Career_Center$|^Central_Committee|^Chemical_|^Coast_"
    r"|^College_Board|^Committee_|^Communications_|^Community_|^Consolidated_"
    r"|^Control_|^Democratic_Convention|^Democratic_Zone$|^District_Attorney$"
    r"|^EXT_|^Education_Commission|^Educational_|^Election_Commissioner"
    r"|^Elementary_|^Emergency_|^Enterprise_|^Exempted_|^Facilities_|^4H_"
    r"|^Geological_|^High_School|^Improvement_|^Independent_Fire|^Land_Commission$"
    r"|^Landscaping_|^Law_Enforcement|^Learning_Community|^Levee_|^Local_"
    r"|^Master_Plan|^Middle_School|^Mountain_|^Municipal_|^Northeast_|^Open_Space"
    r"|^Planning_|^Proposed_|^Public_|^Rapid_Transit|^Regional_Office|^Republican_Area$"
    r"|^Republican_Convention|^Resort_|^Resource_|^River_|^School_|^Service_Area"
    r"|^Special_|^State_Board|^State_House|^State_Legislative|^State_Senate"
    r"|^Superintendent_|^TV_Translator|^TriCity_|^US_Congressional|^Unified_School"
    r"|^Unincorporated_|^Unprotected_|^Ute_Creek|^Vector_|^Vote_By_Mail_Area$"
    r"|^Board_of_Education|^County_)"
)

YEAR_GATED_RE = re.compile(
    r"^(General|Primary|AnyElection|OtherElection|PresidentialPrimary|PRI_BLT)_(\d{4})$"
)
DATE_GATED = {
    "Voters_CalculatedRegDate",
    "Voters_OfficialRegDate",
    "Voters_MovedFrom_Date",
    "FECDonors_NumberOfDonations",
    "FECDonors_TotalDonationsAmount",
    "FECDonors_AvgDonation",
    "FECDonors_AvgDonation_Range",
    "FECDonors_TotalDonationsAmt_Range",
    "FECDonors_LastDonationDate",
}


def classify(col: str) -> tuple[str, str, str]:
    """-> (disposition, time_discipline, reason)"""
    if col in ID_META:
        return "drop_id_meta", "", "identifier / vendor metadata"
    if col not in PII_EXCEPT and (col in PII_EXACT or col in PII_RESIDENCE or col.startswith(PII_PREFIX)):
        return "drop_pii", "", "name / dob / address / phone / device id / geocode"
    if col in PROTECTED or col.startswith(PROTECTED_PREFIX) or PROTECTED_RE.match(col):
        return "drop_protected", "", "gender/ethnicity/religion/language block (decided)"
    if col in PROTECTED_PENDING:
        return "drop_protected_pending", "", "disability — recommend a-priori drop (pending)"
    if UNKNOWN_SEMANTICS_RE.match(col):
        return "drop_unknown", "", "vendor-reserved, undocumented semantics"
    if col in FREETEXT:
        return "drop_freetext", "", "free text / high-cardinality label"
    if col in GEO_KEEP:
        return "keep", "apriori", "coded urbanicity (kept from the geo block)"
    if col in GEO_EXACT or GEO_RE.search(col):
        return "drop_geo", "", "district/jurisdiction/metro geography"
    m = YEAR_GATED_RE.match(col)
    if m:
        return "keep", "year_gated", f"vote history {m.group(2)}: NaN when year >= signup year"
    if col in DATE_GATED:
        return "keep", "date_gated", "gated exactly as build_l2_match.py"
    return "keep", "apriori", "snapshot attribute, acquired independently of signup"


if __name__ == "__main__":
    cc = pd.read_parquet(HERE / "datasets" / "catalog_columns.parquet")
    t = cc[cc.table_name == "int__l2_nationwide_uniform_w_haystaq"][["column_name", "data_type"]].reset_index(
        drop=True
    )
    assert len(t) == 1396, f"expected 1396 columns, got {len(t)}"

    rows = [classify(c) for c in t.column_name]
    t[["disposition", "time_discipline", "reason"]] = pd.DataFrame(rows, index=t.index)

    out = HERE / "datasets" / "l2_block_dispositions.csv"
    t.to_csv(out, index=False)
    print(t.disposition.value_counts().to_string())
    print()
    kept = t[t.disposition == "keep"]
    print("kept, by time discipline:")
    print(kept.time_discipline.value_counts().to_string())
    print("kept, by dtype:")
    print(kept.data_type.value_counts().to_string())
    # sanity: no string column should survive with > modest cardinality — verified against
    # the screening sample in screen_l2_proxy.py, which reports any kept string > 60 levels
    print(f"\nwrote {out}")
    unmatched_strings = kept[kept.data_type == "STRING"]
    print(
        f"kept STRING columns: {len(unmatched_strings)} "
        f"(cardinality verified against the screening sample in step 2)"
    )
