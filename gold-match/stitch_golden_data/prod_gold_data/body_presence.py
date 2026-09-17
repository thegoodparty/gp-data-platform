"""Body-level sub-row presence for candidate 2 class A (DATA-2415).

Answers one question from the office name and the state's universe lists: does this body have sub-level rows in L2?
Type-agnostic on purpose: BallotReady files special districts under the county and place mtfccs while L2 types their
sub-rows on its own vocabulary, so a family-scoped search abstained on correct matches (spec 2.2). Anchor-based on
purpose: whole-name matching fails on L2's abbreviations (REC, WTR, COMM COLL); anchors are the proper-name tokens,
mechanically defined as tokens outside the loaded type vocabulary and a fixed generic list. The test errs toward
"present" so that its failure mode is a miss (today's behavior), never a new false absence.
"""

from __future__ import annotations

import re

# The drafting tokenizer's stop list (.tickets/DATA-2415/instrument/tools/assemble_drafts.py STOP), copied so the
# matcher does not import lane tooling.
STOP_WORDS: frozenset[str] = frozenset(
    {
        "BOARD",
        "COUNCIL",
        "DISTRICT",
        "MEMBER",
        "OF",
        "THE",
        "AND",
        "SEAT",
        "PLACE",
        "POSITION",
        "AT",
        "LARGE",
        "TRUSTEE",
        "COMMISSIONER",
        "COMMISSION",
        "JUDGE",
        "COURT",
        "SCHOOL",
        "CITY",
        "COUNTY",
        "TOWN",
        "TOWNSHIP",
        "VILLAGE",
        "WARD",
        "PRECINCT",
        "DIVISION",
        "GROUP",
        "SUBDISTRICT",
        "AREA",
        "ZONE",
        "SUPERVISOR",
        "MAYOR",
        "CLERK",
        "TREASURER",
        "ATTORNEY",
        "COMMUNITY",
        "DEVELOPMENT",
        "EDUCATION",
        "PUBLIC",
        "SERVICE",
        "UNIFIED",
        "INDEPENDENT",
        "CONSOLIDATED",
        "REGIONAL",
        "UNION",
        "ELEMENTARY",
        "HIGH",
        "ISD",
        "USD",
        "SD",
        "CNTY",
    }
)
# Role, level, compass and school-suffix words that never name a body; measured list, see
# .tickets/DATA-2415/candidate2/dev-measurement/dev_rule_eval_2026-09-17.py (GENERIC).
GENERIC_WORDS: frozenset[str] = STOP_WORDS | frozenset(
    {
        "PARISH",
        "METROPOLITAN",
        "METRO",
        "RURAL",
        "JOINT",
        "COLLEGE",
        "SCHOOLS",
        "GENERAL",
        "MUNICIPAL",
        "TRUSTEES",
        "DIRECTOR",
        "DIST",
        "NO",
        "CISD",
        "ELEM",
        "CENTRAL",
        "NORTH",
        "SOUTH",
        "EAST",
        "WEST",
        "NORTHERN",
        "SOUTHERN",
        "EASTERN",
        "WESTERN",
        "UPPER",
        "LOWER",
        "GREATER",
        "NEW",
        "TA",
        "POS",
        "SUBD",
        "SB",
        "EST",
        "JT",
        "BD",
        # Institutional suffixes: they name the body's legal form, never the body itself.
        "SYSTEM",
        "SYSTEMS",
        "CORPORATION",
        "CORP",
        "AUTHORITY",
        "AGENCY",
        "DEPARTMENT",
        "DEPT",
        "OFFICE",
        "BUREAU",
        "ASSOCIATION",
        "ASSN",
        "COOPERATIVE",
        "COOP",
        "COMPANY",
    }
    | {str(y) for y in range(2018, 2031)}
)
# Every word (split on "_", upper-cased) of every L2 district type name dbt loads
# (dbt/project/macros/l2_district_columns.sql get_l2_district_types(scope="all")), so a type
# word is generic even in a state fixture that happens not to carry that type -- a small
# hand-seeded universe must not mask a real abbreviation false-absence (RECREATION vs REC).
CANONICAL_TYPE_WORDS: frozenset[str] = frozenset(
    {
        "4H",
        "911",
        "ABATEMENT",
        "ADVISORY",
        "AGENCY",
        "AIRPORT",
        "AND",
        "ANNEXATION",
        "APPELLATE",
        "AQUATIC",
        "AREA",
        "ASSESMENT",
        "ASSESSMENT",
        "ASSESSOR",
        "AT",
        "ATTORNEY",
        "BAY",
        "BOARD",
        "BONDS",
        "BOROUGH",
        "CAREER",
        "CEMETERY",
        "CENTER",
        "CENTRAL",
        "CHANCERY",
        "CHEMICAL",
        "CIRCUIT",
        "CITY",
        "COAST",
        "COLLEGE",
        "COMMISSION",
        "COMMISSIONER",
        "COMMITTEE",
        "COMMUNICATION",
        "COMMUNICATIONS",
        "COMMUNITY",
        "CONGRESSIONAL",
        "CONSERVATION",
        "CONSOLIDATED",
        "CONTROL",
        "COORDINATING",
        "CORRECTIONS",
        "COUNCIL",
        "COUNTY",
        "COURT",
        "CREEK",
        "DESIGNATED",
        "DISTRICT",
        "DIVISION",
        "DMA",
        "DRAINAGE",
        "EDUCATION",
        "EDUCATIONAL",
        "ELECTION",
        "ELEMENTARY",
        "EMERGENCY",
        "ENFORCEMENT",
        "ENTERPRISE",
        "EQUALIZATION",
        "EXEMPTED",
        "EXT",
        "FACILITIES",
        "FAMILY",
        "FIRE",
        "FLOOD",
        "FOREST",
        "GARBAGE",
        "GEOLOGICAL",
        "HAMLET",
        "HAZARD",
        "HEALTH",
        "HIGH",
        "HOSPITAL",
        "HOUSE",
        "IMPROVEMENT",
        "INDEPENDENT",
        "IRRIGATION",
        "ISLAND",
        "JUDICIAL",
        "JURY",
        "JUSTICE",
        "JUVENILE",
        "LAND",
        "LANDOWNER",
        "LANDSCAPING",
        "LARGE",
        "LAW",
        "LEARNING",
        "LEGISLATIVE",
        "LEVEE",
        "LIBRARY",
        "LIGHTING",
        "LIVESTOCK",
        "LOCAL",
        "MAGISTRATE",
        "MAINTENANCE",
        "MARKET",
        "MASTER",
        "MAYORAL",
        "MEASURE",
        "MEMORIAL",
        "METRO",
        "METROPOLITAN",
        "MIDDLE",
        "MOSQUITO",
        "MOUNTAIN",
        "MULTI",
        "MUNICIPAL",
        "MUSEUM",
        "NORTHEAST",
        "OF",
        "OFFICE",
        "OPEN",
        "OTHER",
        "PARAMEDIC",
        "PARK",
        "PEACE",
        "PLAN",
        "PLANNING",
        "POLICE",
        "PORT",
        "POWER",
        "PRESERVE",
        "PROTECTION",
        "PUBLIC",
        "RAPID",
        "RECLAMATION",
        "RECONSTRUCTION",
        "RECREATION",
        "RECREATIONAL",
        "REGIONAL",
        "REGULATION",
        "REMOVAL",
        "REPLACEMENT",
        "REPORTING",
        "RESORT",
        "RESOURCE",
        "REVIEW",
        "RIVER",
        "ROAD",
        "RURAL",
        "SANITARY",
        "SCHOOL",
        "SCHOOLS",
        "SENATE",
        "SERVICE",
        "SERVICES",
        "SEWER",
        "SNOW",
        "SOIL",
        "SPACE",
        "SPECIAL",
        "STATE",
        "STORM",
        "STREET",
        "SUB",
        "SUBDISTRICT",
        "SUPER",
        "SUPERINTENDENT",
        "SUPERIOR",
        "SUPERVISORIAL",
        "SUPREME",
        "TAX",
        "THE",
        "TOWN",
        "TOWNSHIP",
        "TRANSIT",
        "TRANSLATOR",
        "TRICITY",
        "TV",
        "UNIFIED",
        "UNINCORPORATED",
        "UNPROTECTED",
        "US",
        "UTE",
        "UTILITY",
        "VECTOR",
        "VILLAGE",
        "VOCATIONAL",
        "WARD",
        "WASTEWATER",
        "WATER",
        "WEED",
        "ZONE",
    }
)
# Generic words that can still name a body (place and body nouns, compass and qualifier words). Every other generic
# word is an office or structure word. Derived rather than hand-copied so a word added to the generic lists later is
# dropped from the fallback by default, which errs toward "present" (fewer tokens a row must carry).
_DESCRIPTIVE_WORDS: frozenset[str] = frozenset(
    {
        "SCHOOL",
        "SCHOOLS",
        "CITY",
        "COUNTY",
        "CNTY",
        "TOWN",
        "TOWNSHIP",
        "VILLAGE",
        "PARISH",
        "WARD",
        "PRECINCT",
        "DIVISION",
        "GROUP",
        "SUBDISTRICT",
        "AREA",
        "ZONE",
        "COMMUNITY",
        "DEVELOPMENT",
        "EDUCATION",
        "PUBLIC",
        "SERVICE",
        "UNIFIED",
        "INDEPENDENT",
        "CONSOLIDATED",
        "REGIONAL",
        "UNION",
        "ELEMENTARY",
        "HIGH",
        "ISD",
        "USD",
        "CISD",
        "SD",
        "ELEM",
        "COLLEGE",
        "METROPOLITAN",
        "METRO",
        "RURAL",
        "JOINT",
        "JT",
        "GENERAL",
        "MUNICIPAL",
        "CENTRAL",
        "NORTH",
        "SOUTH",
        "EAST",
        "WEST",
        "NORTHERN",
        "SOUTHERN",
        "EASTERN",
        "WESTERN",
        "UPPER",
        "LOWER",
        "GREATER",
        "NEW",
    }
)
ROLE_WORDS: frozenset[str] = (STOP_WORDS | GENERIC_WORDS) - _DESCRIPTIVE_WORDS
PROPER_ABBREVIATIONS: dict[str, str] = {
    "MT": "MOUNT",
    "ST": "SAINT",
    "FT": "FORT",
    "PT": "POINT",
    "SPGS": "SPRINGS",
    "HTS": "HEIGHTS",
    "VLY": "VALLEY",
    "CTR": "CENTER",
}
SUB_LEVEL_TYPE_PATTERN = re.compile(
    r"sub|ward|zone|division|precinct|trustee|council_commissioner|board_district|commissioner_district|"
    r"supervisorial|justice",
    re.IGNORECASE,
)
MIN_COMPOUND_LEN = 4  # HEALTHCARE carries CARE; a 3-letter anchor inside a longer token is coincidence


def tokens(text: str) -> list[str]:
    out = []
    for t in re.sub(r"[^A-Za-z0-9]+", " ", text or "").upper().split():
        out.append(str(int(t)) if t.isdigit() else t)
    return out


def type_words(district_types: list[str]) -> frozenset[str]:
    return frozenset(w.upper() for t in set(district_types) for w in t.split("_") if w)


def _body(office_name: str) -> str:
    return re.sub(r"\([^)]*\)", " ", re.sub(r" - .*$", "", office_name or ""))


def anchor_tokens(office_name: str, generic: frozenset[str]) -> frozenset[str]:
    body = {PROPER_ABBREVIATIONS.get(t, t) for t in tokens(_body(office_name))}
    anchors = body - generic
    return frozenset(anchors) if anchors else frozenset(body - ROLE_WORDS)


def is_sub_level_type(
    district_type: str, known_sub_types: frozenset[str] = frozenset(), only_known_sub_types: bool = False
) -> bool:
    if only_known_sub_types:
        return district_type in known_sub_types
    return district_type in known_sub_types or bool(SUB_LEVEL_TYPE_PATTERN.search(district_type or ""))


def _carries(row_tokens: set[str], anchor: str) -> bool:
    return anchor in row_tokens or (len(anchor) >= MIN_COMPOUND_LEN and any(anchor in r for r in row_tokens))


def body_has_sub_rows(
    office_name: str,
    district_types: list[str],
    district_names: list[str],
    known_sub_types: frozenset[str] = frozenset(),
    only_known_sub_types: bool = False,
) -> bool:
    generic = GENERIC_WORDS | CANONICAL_TYPE_WORDS | type_words(district_types)
    anchors = anchor_tokens(office_name, generic)
    if not anchors:
        return True  # nothing to test against: err toward present
    for t, n in zip(district_types, district_names, strict=True):
        if not is_sub_level_type(t, known_sub_types, only_known_sub_types):
            continue
        row = {PROPER_ABBREVIATIONS.get(x, x) for x in tokens(re.sub(r"\([^)]*\)", " ", n))}
        if all(_carries(row, a) for a in anchors):
            return True
    return False
