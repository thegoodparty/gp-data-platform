"""L2-to-BallotReady district matcher.

Named after the warehouse objects it reads and writes
(int__l2_br_match_pending_offices, llm_l2_br_match_results) so the dbt side
and the omni side share one vocabulary.

The matcher core -- the district and query embedding text, the menu
construction, the LLM prompt, response schema and Braintrust identifiers --
is an owner-decided constraint, not reviewable, and is reproduced here, not
redesigned. What changed: inputs come from Databricks instead of laptop
pickles, embeddings live in memory for the run and are discarded, and a run
persists only matches and abstentions -- a technical error fails the run
instead of being recorded as a match.

This module writes nothing. `run()` returns terminal results for the caller
to print or inspect; the Databricks write path lives beside it in
l2_br_match_writer.py.
"""

import argparse
import asyncio
import functools
import math
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from shared.braintrust import build_cached_prompt, cache_prompt, flush_logs, init_braintrust
from shared.databricks_client import DatabricksClient
from shared.llm_gemini import GeminiEmbeddingClient
from shared.llm_gemini_3 import Gemini3Client, GeminiModelType, ThinkingLevel
from shared.logger import get_logger

PENDING_OFFICES_TABLE = "int__l2_br_match_pending_offices"
DISTRICT_UNIVERSE_TABLE = "int__l2_district_universe"

MENU_SIZE = 13
STATE_QUERY_INSERT_INDEX = 10  # 11th slot

# Sized for the LLM client's max_connections=1200 / max_keepalive_connections=300
# below (unchanged, FROZEN). The deleted matcher owned a pool at this same size.
THREAD_POOL_SIZE = 1500

_STATE_CODE_RE = re.compile(r"^[A-Z]{2}$")


@dataclass
class DistrictCandidate:
    """One menu entry: an L2 district.

    l2_state comes from the district's own row in int__l2_district_universe,
    not from the office being matched: a district key is (state, type,
    name), and a position can change state even though the two are equal
    today -- so the source of this field matters even when its value
    doesn't yet.
    """

    l2_state: str
    l2_district_type: str
    l2_district_name: str


@dataclass
class MatchResult:
    """One office's terminal outcome, and there are only two of them: the
    three district fields are populated (a match) or all three are None (an
    attempt that found nothing). No third state and no status column -- a
    technical error raises instead of producing a MatchResult.

    Carries five of llm_l2_br_match_results' six columns. The sixth,
    `attempted_at`, is deliberately absent: one run stamps one value across
    every row it writes and the orchestrator passes it in, so minting it
    per result is exactly what that contract prevents. It must also never
    default to load time -- the baseline run stamps it with the date an
    office was actually matched, not today's date, and getting that wrong
    shifts every 30-day clock. No `llm_reason`, no `embeddings`, no
    `alternative_matches`, no `is_exact_district_match`: the results table
    has no column for any of them.
    """

    br_database_id: int
    l2_state: str | None
    l2_district_type: str | None
    l2_district_name: str | None
    confidence: int | None


@dataclass
class _StateUniverse:
    """One state's embedded district universe, held in memory only and
    never written to disk. Parallel lists, one entry per district row.
    """

    embeddings: np.ndarray
    states: list[str]
    district_types: list[str]
    district_names: list[str]


@dataclass(frozen=True)
class _GeographyVerdict:
    """One office's geography classification: whether it
    abstains before any embedding or LLM call, which of the state's
    already-built universe indices the frozen menu mechanics may rank,
    and the sentence `districts_text` carries for it.

    `eligible_indices=None` means no restriction (pass-through, judicial
    with no vocabulary in the state -- handled by `abstain` instead --
    or a gated-off school whole-assertion): callers must check for `None`
    rather than compare against a full index range, so a state whose
    universe shrinks between runs is never mistaken for "nothing
    eligible". `verdict_sentence` is `None` unless R2 actually fired.
    """

    abstain: bool
    eligible_indices: frozenset[int] | None
    verdict_sentence: str | None


def _normalize_state(value: Any) -> str:
    """Strip and upper-case one value.

    Callers must normalize the whole COLUMN before grouping or keying
    anything by it, not just the key or the lookup side alone -- wrapping
    only the groupby key here once let two raw spellings of one state
    collapse into one dict key, with the later group silently overwriting
    the earlier's districts. Normalizing the column first means every
    consumer (the dict key, the embedding text, the candidate,
    `MatchResult.l2_state`) sees the same canonical value by construction.
    """
    return str(value).strip().upper()


def _validate_states_filter(states: list[str]) -> list[str]:
    """Normalize and validate a `states` filter (e.g. --states) and return
    the normalized list. Must run before EITHER SQL string that depends on
    it is built.

    `load_pending_offices` splices this straight into its own WHERE clause.
    `_validate_pending_offices` cannot protect that query: it validates the
    ROWS the query already returned, after it has already run -- it only
    protects the later `load_district_universe` call, built from the
    result. A non-canonical value here (e.g. "DE') or 1=1 --") would
    otherwise reach `load_pending_offices`'s WHERE clause unescaped and
    read far more than the requested state.
    """
    normalized = [_normalize_state(s) for s in states]
    bad = sorted({s for s in normalized if not _STATE_CODE_RE.match(s)})
    if bad:
        raise ValueError(f"Not a canonical two-letter state code: {bad}")
    return normalized


def _validate_pending_offices(pending_df: pd.DataFrame) -> None:
    """Fail closed on a state or a name a later step would otherwise accept
    silently. `state` must already be normalized (see `_normalize_state`).

    This validates the ROWS `load_pending_offices`'s query already
    returned, so it protects the LATER `load_district_universe` call (built
    from these rows' `state` values) -- not `load_pending_offices`'s own
    query, which is already built and executed by the time this runs. See
    `_validate_states_filter` for that.

    A non-canonical state here would reach `load_district_universe`'s SQL
    IN-clause unescaped -- that value is warehouse data, not operator
    input, so one apostrophe in it breaks or extends the query. A blank
    name still embeds cleanly (`"race name: "`) and gets back a real,
    arbitrary match -- worse than a bad state, because a match is a link
    the pending list's own 30-day rule never reopens.
    """
    bad_state_mask = ~pending_df["state"].str.match(_STATE_CODE_RE)
    if bad_state_mask.any():
        bad_states = sorted(pending_df.loc[bad_state_mask, "state"].unique())
        raise ValueError(f"Pending offices carry a non-canonical state code: {bad_states}")

    blank_name_mask = pending_df["name"].isna() | (pending_df["name"].astype(str).str.strip() == "")
    if blank_name_mask.any():
        raise ValueError(f"{int(blank_name_mask.sum())} pending office(s) have a blank or null name")


def _validate_district_universe(universe_df: pd.DataFrame) -> None:
    """Fail closed on a null or blank state, district type, or name.

    pandas `groupby` defaults to `dropna=True`, so a null `state_postal_code`
    is silently dropped rather than caught -- that district simply never
    enters any state's menu, with no error. A null `district_type` or
    `district_name` is not caught by the groupby at all: it survives into
    the embedding text as the literal string "nan" and, if the LLM ever
    selects it, into `MatchResult.l2_district_name` as a float -- on a row
    whose docstring says that field is None only when the attempt found
    nothing, and where a populated value is the whole signal that the
    office matched. Rule 3 then re-offers that office on every dbt build,
    re-paying the LLM cost forever.
    """
    for column in ("state_postal_code", "district_type", "district_name"):
        blank_mask = universe_df[column].isna() | (universe_df[column].astype(str).str.strip() == "")
        if blank_mask.any():
            raise ValueError(f"{int(blank_mask.sum())} district universe row(s) have a null or blank {column}")


def _district_embedding_text(state: str, district_type: str, district_name: str) -> str:
    """FROZEN (vector_store_generator.py's create_embedding_texts). Reproduce
    character-for-character, including the spacing and punctuation: this text
    is what gets embedded, so a drift here silently changes every similarity
    score and every match against the January holdout.
    """
    return f"state: {state}, district type: {district_type}, district name: {district_name}"


def _require_integral(value: Any, field_name: str) -> int:
    """Reject a bool, a non-numeric type, a non-finite float, and any value
    that is not a whole number; return a clean `int` otherwise.

    A bool is an `int` subclass in Python, so `isinstance(value, bool)` must
    be checked ahead of the general numeric check, or `True` would silently
    pass as `1`. The response schema declares both the selection and the
    confidence as a bare "number", and the LLM client does no client-side
    validation of its own, so `3.9` would otherwise truncate to `3` --
    silently recording the model's 4th choice as its 3rd -- and a string
    like `'95'` would pass straight through into a column typed as an
    integer.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a non-boolean numeric value: {value!r}")
    if not math.isfinite(value):
        raise ValueError(f"{field_name} must be finite: {value!r}")
    if not float(value).is_integer():
        raise ValueError(f"{field_name} must be a whole number: {value!r}")
    return int(value)


def _selection_from_response(response: dict[str, Any], num_candidates: int) -> tuple[int, int]:
    """Extract (selected index, confidence) from a raw LLM response.

    Raises ValueError on a missing, non-numeric, non-integral, boolean, or
    out-of-range value for either field. An abstention (index 0) is a
    judgment the model made and closes the office on the pending list for
    30 days; a schema violation is not a judgment, so it must fail the run
    rather than being coerced into an abstention.

    Both response reads sit in the same try block, so the raise happens
    before either key is read individually and before a `None` response
    (TypeError on subscript) reaches any attribute access -- a guard that
    caught a bad index and then read a second key unguarded right after is
    what this replaces. `OverflowError` is in the tuple too: `json.loads`
    yields an arbitrary-precision int for a bare "number" field, and
    `math.isfinite` raises `OverflowError`, not `ValueError`, on one too
    large to convert to a float.
    """
    try:
        selected_index = _require_integral(response["selected_candidate_number"], "selected_candidate_number")
        confidence = _require_integral(response["selection_confidence"], "selection_confidence")
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"Malformed LLM response, expected a numeric selection and confidence: {response!r}") from exc

    if not 0 <= selected_index <= num_candidates:
        raise ValueError(f"selected_candidate_number {selected_index} out of bounds for {num_candidates} candidates")
    if not 0 <= confidence <= 100:
        raise ValueError(f"selection_confidence {confidence} out of bounds [0, 100]")

    return selected_index, confidence


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {value!r}")
    return parsed


def _canonical_state_arg(value: str) -> str:
    """argparse `type=` for --states: validate and normalize at parse time,
    before `main` ever constructs a matcher or either query is built.
    """
    try:
        return _validate_states_filter([value])[0]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


# -- Geography filters -------------------------------------
#
# Fields BallotReady already ships remove impossible options from the menu
# before the LLM sees it, and add context to the prompt. No field asserts
# a match on its own, and denial is always within an office's own family,
# so a correct out-of-family answer is never touched. If the filters leave
# nothing eligible, the office abstains before the LLM.

# `False` because BR cannot distinguish a residency zone (every voter in
# the jurisdiction votes on every numbered seat) from a genuinely zoned
# electorate on these fields alone. Gated off means no denial AND no
# steering sentence -- the off arm must be today's behavior, or the
# holdout's before/after comparison is contaminated. The holdout runs both
# arms via --enable-school-whole-assertion; this constant is only the
# default.
SCHOOL_WHOLE_ASSERTION_ENABLED = False

_PARTY_COMMITTEE_MTFCC = "X0024"  # R0: no public electoral district exists
_STATEWIDE_MTFCC = "G4000"
_JUDICIAL_TYPE_PREFIX = "Judicial_"
_SOLE_SUPREME_JUDICIAL_TYPE = "Judicial_Supreme_Court_District"

# Parent (whole-jurisdiction) and sub-level (numbered zone) type sets per
# family, enumerated from the canonical vocabulary in
# gp-data-platform/dbt/project/macros/l2_district_columns.sql
# (get_l2_district_types(scope="all")), not hand-picked. A type added
# there needs this table re-checked -- that coupling is why this comment
# names the macro path instead of restating the enumeration's rationale.
_FAMILY_PARENT_TYPES: dict[str, frozenset[str]] = {
    "county": frozenset({"County"}),
    "place": frozenset({"City", "Village", "Borough", "Hamlet_Community_Area"}),
    "school": frozenset(
        {
            "School_District",
            "Unified_School_District",
            "City_School_District",
            "County_Unified_School_District",
            "Elementary_School_District",
            "High_School_District",
            "Middle_School_District",
            "Exempted_Village_School_District",
            "Board_of_Education_District",
            "County_Board_of_Education_District",
        }
    ),
    "county_subdivision": frozenset({"Township", "Town_District"}),
}
_FAMILY_SUB_TYPES: dict[str, frozenset[str]] = {
    "county": frozenset(
        {"County_Commissioner_District", "County_Supervisorial_District", "County_Legislative_District"}
    ),
    "place": frozenset({"City_Ward", "City_Council_Commissioner_District", "Village_Ward", "Borough_Ward"}),
    "school": frozenset(
        {
            "School_Subdistrict",
            "Unified_School_SubDistrict",
            "Elementary_School_SubDistrict",
            "High_School_SubDistrict",
            "Board_of_Education_SubDistrict",
            "County_Board_of_Education_SubDistrict",
            "School_Board_District",
        }
    ),
    "county_subdivision": frozenset({"Township_Ward", "Town_Ward"}),
}
# Census geo_id length of the family's PARENT (whole-jurisdiction) id; a
# genuine sub-unit id is strictly longer. Measured:
# county 5 (G4020/X0005), place 7 (G4110/G4210/X0001), school 7
# (G5420/G5400/G5410/X0102, the LEA id), county subdivision 10 (G4040).
_FAMILY_PARENT_GEOID_LENGTH: dict[str, int] = {
    "county": 5,
    "place": 7,
    "school": 7,
    "county_subdivision": 10,
}
_FAMILY_BY_MTFCC: dict[str, str] = {
    "G4020": "county",
    "X0005": "county",
    "G4110": "place",
    "G4210": "place",
    "X0001": "place",
    "G5420": "school",
    "G5400": "school",
    "G5410": "school",
    "X0102": "school",
    "G4040": "county_subdivision",
}

# The measured top-20 sub_area_name vocabulary splits into seat designators
# (a whole-jurisdiction seat number) and territory words (a real slice).
# Reading only, never a classifier input by settled design ruling: it flavors the geography sentence's wording and nothing else.
_SEAT_DESIGNATOR_SUB_AREA_NAMES = frozenset({"Seat", "Position", "Place", "Office", "Group", "Post"})

# Background knowledge (Census MTFCC + BR's own X-code extensions), used
# only for descriptive prompt context; an unmapped code falls back to its
# bare value rather than raising, since it still must reach the prompt.
_MTFCC_PLAIN_ENGLISH: dict[str, str] = {
    "G4000": "state",
    "G4020": "county",
    "G4040": "county subdivision (township or town)",
    "G4110": "incorporated place (city, village, or borough)",
    "G4210": "census-designated place (unincorporated community)",
    "G5200": "congressional district",
    "G5210": "state legislative district, upper chamber",
    "G5220": "state legislative district, lower chamber",
    "G5400": "elementary school district",
    "G5410": "secondary school district",
    "G5420": "unified school district",
    "X0001": "city council district",
    "X0005": "county commission district",
    "X0024": "party committee area",
    "X0102": "school subdistrict",
}
_MAX_PROMPT_FIELD_LENGTH = 200  # vendor text entering a prompt; capped, not trusted


def _geo_id_family_format(geo_id: str | None, parent_length: int) -> str:
    """Classify geo_id against a family's parent (whole-jurisdiction)
    Census id length: "whole" (exactly the parent format), "slice" (a
    real sub-unit id, strictly longer), or "malformed" (missing, shorter
    than the parent format, or not digits where the family's Census id
    must be numeric). Only the first `parent_length` characters must be
    digits -- a slice id's own suffix need not be (Compton's trustee-area
    geo_id ends in a letter), so this checks the parent prefix only.
    """
    if not geo_id:
        return "malformed"
    prefix = geo_id[:parent_length]
    if len(prefix) < parent_length or not prefix.isdigit():
        return "malformed"
    return "whole" if len(geo_id) == parent_length else "slice"


def _sanitize_prompt_text(value: str | None) -> str | None:
    """Collapse embedded newlines to spaces and cap length. This is
    vendor text entering a prompt, not operator input -- a malformed
    sub_area value must not be able to break the block's structure or
    blow past a reasonable field size.
    """
    if not value:
        return None
    return " ".join(str(value).split())[:_MAX_PROMPT_FIELD_LENGTH] or None


def _build_geography_block(
    mtfcc: str,
    sub_area_name: str | None,
    sub_area_value: str | None,
    has_unknown_boundaries: bool,
    verdict_sentence: str | None,
) -> str:
    """The delimited block appended to `districts_text`: context
    only, never an assertion. Three lines always; a fourth only when R2
    actually fired, which is what `verdict_sentence` being non-`None`
    means.
    """
    name = _sanitize_prompt_text(sub_area_name)
    value = _sanitize_prompt_text(sub_area_value)
    if name and value:
        sub_area_line = f"{name}: {value}"
    else:
        sub_area_line = name or value or "none recorded"

    boundary_line = (
        "unknown (BR is showing a higher-level area in place of this office's real territory)"
        if has_unknown_boundaries
        else "known (this is the office's own territory)"
    )

    lines = [
        "Office geography (context only -- it does not decide the match):",
        f"- Territory class: {_MTFCC_PLAIN_ENGLISH.get(mtfcc, f'mtfcc {_sanitize_prompt_text(mtfcc)}')}",
        f"- Sub-area: {sub_area_line}",
        f"- Boundary geometry: {boundary_line}",
    ]
    if verdict_sentence:
        lines.append(f"- {verdict_sentence}")
    return "\n".join(lines)


def _classify_office_geography(
    mtfcc: str,
    is_judicial: bool,
    has_unknown_boundaries: bool,
    geo_id: str | None,
    sub_area_name: str | None,
    sub_area_value: str | None,
    state_district_types: list[str],
    school_whole_assertion_enabled: bool = SCHOOL_WHOLE_ASSERTION_ENABLED,
) -> _GeographyVerdict:
    """Map one office's geography fields to an abstain-or-restrict verdict
    against its state's already-built universe. Classified by
    the FIRST rule that applies:

    R0: a party-committee seat has no public electoral district.
    R1: `is_judicial` owns the office outright, by abstain (no judicial
        vocabulary in the state and not statewide-shaped, or the state's
        only judicial type is the wrong level) or by a judicial-only-plus-
        State menu. It never falls through to R2 -- the flag-vs-class
        mismatches this excludes follow the flag, not the mtfcc.
    R2: a known family (county, place, school, county subdivision) with
        at least one sub_area field present classifies by the boundary
        flag and geo_id format, never by sub_area vocabulary. A
        slice-asserted office whose state carries none of the family's
        sub-level types abstains rather than falling back to an
        out-of-family or statewide answer (the v1 critical fix: within-
        family denial alone can never empty the pool on its own).

    `state_district_types` is positional against the caller's own
    embedded universe lists (`_StateUniverse.district_types`), so the
    returned indices index directly into them.
    """
    if mtfcc == _PARTY_COMMITTEE_MTFCC:
        return _GeographyVerdict(abstain=True, eligible_indices=frozenset(), verdict_sentence=None)

    if is_judicial:
        judicial_types = {t for t in state_district_types if t.startswith(_JUDICIAL_TYPE_PREFIX)}
        is_statewide_shaped = mtfcc == _STATEWIDE_MTFCC
        if not is_statewide_shaped and (not judicial_types or judicial_types == {_SOLE_SUPREME_JUDICIAL_TYPE}):
            return _GeographyVerdict(abstain=True, eligible_indices=frozenset(), verdict_sentence=None)
        eligible = frozenset(i for i, t in enumerate(state_district_types) if t in judicial_types or t == "State")
        return _GeographyVerdict(abstain=False, eligible_indices=eligible, verdict_sentence=None)

    has_sub_area = bool(sub_area_name) or bool(sub_area_value)
    family = _FAMILY_BY_MTFCC.get(mtfcc) if has_sub_area else None
    if family is None:
        return _GeographyVerdict(abstain=False, eligible_indices=None, verdict_sentence=None)

    if has_unknown_boundaries:
        level = "slice"
    else:
        level = _geo_id_family_format(geo_id, _FAMILY_PARENT_GEOID_LENGTH[family])
        if level == "malformed":
            return _GeographyVerdict(abstain=False, eligible_indices=None, verdict_sentence=None)

    # Vocabulary flavors the sentence only; classification never reads it.
    is_seat_word = bool(sub_area_name) and sub_area_name.strip() in _SEAT_DESIGNATOR_SUB_AREA_NAMES
    noun = "seat label" if is_seat_word else "sub-area"
    if level == "slice":
        sub_types_present = _FAMILY_SUB_TYPES[family] & set(state_district_types)
        if not sub_types_present:
            return _GeographyVerdict(abstain=True, eligible_indices=frozenset(), verdict_sentence=None)
        # Two slice provenances, two honest sentences: with the flag set the
        # displayed geometry is a stand-in (the boundary line in the block
        # says so), and claiming the geography IS a genuine sub-area would
        # contradict it in the same prompt.
        if has_unknown_boundaries:
            sentence = (
                f"This office is a districted {noun} within its jurisdiction; the displayed "
                "geometry is a stand-in for its real, unmapped territory."
            )
        else:
            sentence = f"This office's geography is a genuine {noun} within its jurisdiction, not the whole area."
        deny = _FAMILY_PARENT_TYPES[family]
    else:  # "whole"
        sentence = (
            f"This office's geography covers the entire jurisdiction; the {noun} is a residency "
            "zone or seat designation, not a smaller territory."
        )
        if family == "school" and not school_whole_assertion_enabled:
            # Gated off: no denial and no steering sentence. Shipping the
            # whole-jurisdiction claim into the prompt while "off" would be
            # a soft denial in the exact direction the gate holds back.
            return _GeographyVerdict(abstain=False, eligible_indices=None, verdict_sentence=None)
        deny = _FAMILY_SUB_TYPES[family]

    eligible = frozenset(i for i, t in enumerate(state_district_types) if t not in deny)
    return _GeographyVerdict(abstain=False, eligible_indices=eligible, verdict_sentence=sentence)


class L2BrMatcher:
    """Matches BallotReady offices to L2 districts. See module docstring for
    what is frozen and what changed in this PR.
    """

    def __init__(self, catalog: str = "goodparty_data_catalog", schema: str = "dbt"):
        self.logger = get_logger(__name__)
        self.databricks = DatabricksClient()

        target_concurrency = 1200
        self.llm = Gemini3Client(
            default_model=GeminiModelType.FLASH_3,
            default_temperature=0.0,
            thinking_level=ThinkingLevel.MINIMAL,
            max_connections=target_concurrency,
            max_keepalive_connections=target_concurrency // 4,
            max_retries=11,
            base_delay=1.0,
        )
        self.embedding_client = GeminiEmbeddingClient(max_retries=11, base_delay=1.0)

        # An explicit pool, not the loop's default executor (asyncio.to_thread's
        # min(32, cpu_count + 4)), so the httpx pool above actually sees the
        # concurrency it is sized for.
        self._executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)

        self._embedding_cost_baseline = 0.0
        self._llm_cost_baseline = 0.0

        self.pending_offices_path = f"{catalog}.{schema}.{PENDING_OFFICES_TABLE}"
        self.district_universe_path = f"{catalog}.{schema}.{DISTRICT_UNIVERSE_TABLE}"

        self._universe_by_state: dict[str, _StateUniverse] = {}

        init_braintrust(project="stitch-golden-data")
        self._init_prompt_cache()

    def _init_prompt_cache(self) -> None:
        self._prompt_name = "stitch-golden-data-matcher"
        prompt_obj = cache_prompt(self._prompt_name)
        if prompt_obj is not None:
            self.logger.info("Braintrust prompt cached for stitch-golden-data-matcher")
        else:
            self.logger.warning("Braintrust prompt not available, using fallback")

    async def _run_in_pool(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Dispatch a blocking call through this instance's own thread pool,
        never the event loop's default executor -- see the pool-sizing
        comment in __init__.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, functools.partial(func, *args, **kwargs))

    # -- Read path -----------------------------------------------------

    def load_pending_offices(self, states: list[str] | None = None, limit: int | None = None) -> pd.DataFrame:
        """Read the worklist, including the geography columns the
        filters classify on (`mtfcc`, `geo_id`, `sub_area_name`,
        `sub_area_value`, `is_judicial`, `has_unknown_boundaries`).

        `states=None` means no filter (read every state); `states=[]` is an
        explicit empty selection and reads nothing -- a bare `if states:`
        cannot tell those apart, and treated them oppositely from
        `build_universe`, so a programmatic `run(states=[])` (a sharding
        loop, say) would silently read and bill the entire backlog instead
        of doing nothing.

        `--states` is validated (`_validate_states_filter`) and normalized
        BEFORE the query is built, not after it returns: a non-canonical
        value spliced unescaped into the WHERE clause below can widen or
        break the query, and nothing downstream can catch that after the
        fact.

        Deterministic order (by br_database_id): with no ORDER BY, `--limit
        N` would return a different N offices on every invocation, and
        comparability against the January holdout is what this work is
        measured on.

        State values in the RESULT are normalized (see `_normalize_state`)
        and validated (see `_validate_pending_offices`) here, since this is
        the sole place any caller reads them.
        """
        if states is not None and not states:
            return pd.DataFrame(
                columns=[
                    "br_database_id",
                    "name",
                    "state",
                    "mtfcc",
                    "geo_id",
                    "sub_area_name",
                    "sub_area_value",
                    "is_judicial",
                    "has_unknown_boundaries",
                ]
            )

        where_clause = ""
        if states is not None:
            states_str = "', '".join(_validate_states_filter(states))
            # upper(trim(...)) on the COLUMN, not just on the literals. The
            # literals are already canonical by this point; the column is
            # warehouse data. Filtering the raw column discards a 'de' row
            # before _normalize_state below can ever see it, and the result
            # is a silent empty worklist rather than an error.
            where_clause = f"where upper(trim(state)) in ('{states_str}')"
        limit_clause = f"limit {limit}" if limit is not None else ""

        query = f"""
        select
            br_database_id, name, state, mtfcc, geo_id, sub_area_name, sub_area_value,
            is_judicial, has_unknown_boundaries
        from {self.pending_offices_path}
        {where_clause}
        order by br_database_id
        {limit_clause}
        """
        pending_df = self.databricks.execute_query(query)
        if pending_df.empty:
            return pending_df

        pending_df = pending_df.copy()
        pending_df["state"] = pending_df["state"].map(_normalize_state)
        # One normalization site for the geography columns, same principle as
        # `state` above. A batch whose nullable column comes back entirely
        # NULL (a small --states/--limit slice) is inferred float64 by
        # pandas, and NaN is TRUTHY: it would subscript-crash the geo_id
        # format check, classify a no-sub_area office as carrying one, and
        # render the literal string "nan" into a prompt. Coercing here means
        # every consumer sees None/bool by construction.
        # BR has shipped the literal string "null" for this source family
        # before (gp-api normalizes the same sentinel); zero pending rows
        # carry it today, so this is insurance at the site that already
        # exists, not a reachable-bug fix.
        for column in ("mtfcc", "geo_id", "sub_area_name", "sub_area_value"):
            coerced = pending_df[column].astype(object).where(pd.notna(pending_df[column]), None)
            pending_df[column] = coerced.map(
                lambda v: None if v is None or str(v).strip().lower() in ("", "null") else v
            )
        for column in ("is_judicial", "has_unknown_boundaries"):
            pending_df[column] = (
                pending_df[column].astype(object).where(pd.notna(pending_df[column]), False).astype(bool)
            )
        _validate_pending_offices(pending_df)
        return pending_df

    def load_district_universe(self, states: list[str]) -> pd.DataFrame:
        """Read the menu source for exactly the states the worklist needs."""
        states_str = "', '".join(_normalize_state(s) for s in states)
        # upper(trim(...)) for the same reason as load_pending_offices: a
        # non-canonical state_postal_code would otherwise drop that state's
        # districts from the menu entirely.
        query = f"""
        select state_postal_code, district_type, district_name
        from {self.district_universe_path}
        where upper(trim(state_postal_code)) in ('{states_str}')
        """
        return self.databricks.execute_query(query)

    async def build_universe(self, states: list[str], embedding_batch_size: int) -> None:
        """Read and embed int__l2_district_universe for exactly these
        states, held in memory only.

        Clears any prior entries first: without this, a second run() whose
        universe read comes back empty (a dbt rebuild in flight, a delivery
        gap) would leave the FIRST run's entries in place, `groupby` would
        yield no groups, and run()'s missing-states guard would find
        nothing missing even though every entry is now stale.

        Normalizes the state COLUMN once, before the groupby -- not just the
        group key. Normalizing only the key let two raw spellings of one
        state (`'DE'`, `'de'`) collapse into one dict key, with the later
        group silently overwriting the earlier's districts, and still left
        the unnormalized value flowing into the frozen embedding text, the
        candidate, and `MatchResult.l2_state` (which the pending list's rule
        3 join then can never match). Normalizing the column first means
        the key, the text, the candidate and the result all carry the same
        canonical value by construction, not by three call sites agreeing.
        """
        self._universe_by_state = {}
        if not states:
            return
        universe_df = self.load_district_universe(states)
        universe_df = universe_df.copy()
        if not universe_df.empty:
            _validate_district_universe(universe_df)
            universe_df["state_postal_code"] = universe_df["state_postal_code"].map(_normalize_state)

        # Before the embedding loop, not after it. A state the current L2
        # delivery does not carry is an infrastructure failure that aborts the
        # run and persists nothing, so checking it once every OTHER state has
        # already been embedded means paying the full embedding bill for a run
        # that cannot finish. The gap this catches is not hypothetical: prod
        # rebuilds this model several times an hour, so a delivery can land
        # mid-run.
        present = set(universe_df["state_postal_code"]) if not universe_df.empty else set()
        missing = sorted(s for s in {_normalize_state(s) for s in states} if s not in present)
        if missing:
            raise ValueError(f"No district universe entry for state(s): {missing}")

        for state, group in universe_df.groupby("state_postal_code"):
            await self._embed_state_universe(state, group, embedding_batch_size)

    async def _embed_state_universe(self, state: str, district_rows: pd.DataFrame, embedding_batch_size: int) -> None:
        """Embed one state's district universe, batched, dispatched through
        this instance's own thread pool.

        No `parallel=` argument: `create_embeddings` never reads one. Passing
        `parallel=True` here (as the producer did) looks like it selects the
        document path, and it does not -- `len(texts) > 1` alone does, at
        shared/llm_gemini.py:937. Leaving it in implies a second lever exists
        for the very dispatch the query path depends on there being only one
        of.

        `create_embeddings` does `asyncio.run(...)` internally for any
        multi-text input (shared/llm_gemini.py:941), and every state's
        universe has at least two rows -- int__l2_district_universe.sql
        emits one synthetic `district_type='State'` row per state
        unconditionally, on top of that state's real districts, and every
        state carries real districts today -- so calling it directly
        from this coroutine would raise `RuntimeError: asyncio.run() cannot
        be called from a running event loop`. vector_store_generator.py:204 is the
        only other caller of this same document path and already wraps the
        identical call in a thread for exactly this reason.

        The column-name trap: the embedding text is assembled from three
        values in a fixed order, so a positional read that silently swaps
        two of them would produce a plausible string and a different
        embedding. Read each column by its explicit name.
        """
        states: list[str] = []
        district_types: list[str] = []
        district_names: list[str] = []
        texts: list[str] = []
        for _, row in district_rows.iterrows():
            row_state = row["state_postal_code"]
            row_district_type = row["district_type"]
            row_district_name = row["district_name"]
            states.append(row_state)
            district_types.append(row_district_type)
            district_names.append(row_district_name)
            texts.append(_district_embedding_text(row_state, row_district_type, row_district_name))

        # Left at the client defaults (max_concurrent_batches=2,
        # rate_limit_delay=2.0). Only one of those is an argued choice.
        #
        # max_concurrent_batches=2 is NOT tuned, and it is the throughput
        # ceiling: shared/llm_gemini.py:797 sizes the semaphore from it, and
        # :822 posts each text in a batch SEQUENTIALLY inside that slot, so at
        # most two requests are ever in flight no matter what
        # --embedding-batch-size is set to. Raising the batch size makes each
        # slot's work longer, not wider. The stagger at :805-811 also sleeps
        # while holding the semaphore. The producer this replaces used 800.
        # Changing it alters no vector, since taskType and text are unaffected,
        # so it is a throughput question for the measured run rather than
        # something to guess at here.
        #
        # rate_limit_delay is the argued one. It is not just
        # the inter-batch pause: shared/llm_gemini.py:794 seeds the 429
        # retry backoff from it, :867 computes each retry's sleep as
        # current_delay * 2**attempt, and :844 ratchets current_delay back
        # toward this floor on every success -- so a low value (0.01, tried
        # in an earlier round to match vector_store_generator.py's settings)
        # collapses the total retry sleep by orders of magnitude, worst
        # exactly when concurrency is high enough to make a 429 wave likely.
        # Recomputed rather than estimated: :857 doubles current_delay on
        # every 429 up to a 30s cap and :869 then sleeps
        # current_delay * 2**attempt, so a sustained 429 wave at the default
        # 2.0 floor sleeps 4+16+64+240+480+960+1920+3840+7680+15360 =
        # 30,564s across the ten waits before the eleventh attempt raises.
        # At a 0.01 floor the same sequence is about 7,000s. An earlier
        # revision of this comment said 4,000s and 20s; both were wrong,
        # because they assumed a constant delay rather than the ratchet. Tuning these wants a measured run, not a
        # guess; that measured run is what the write-path PR and the
        # supervised cutover produce.
        embeddings = await self._run_in_pool(
            self.embedding_client.create_embeddings,
            texts,
            batch_size=embedding_batch_size,
        )
        self._universe_by_state[state] = _StateUniverse(
            embeddings=embeddings,
            states=states,
            district_types=district_types,
            district_names=district_names,
        )

    # -- Matcher core (FROZEN, owner-decided, not reviewable) -----------

    @staticmethod
    def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

    async def _embed_query_texts(self, queries: list[str]) -> list[np.ndarray]:
        """Embed each query with its own create_embeddings call, one text
        per call -- never batch these.

        create_embeddings dispatches purely on `len(texts) == 1`
        (shared/llm_gemini.py:937): a multi-text call routes through
        _create_embeddings_parallel, which sets taskType=RETRIEVAL_DOCUMENT
        (shared/llm_gemini.py:831, the file's only occurrence); a
        single-text call goes to _create_single_embedding, which sets no
        taskType. The `parallel`/`batch_size` arguments play no part in that
        choice. So documents live in RETRIEVAL_DOCUMENT space and queries
        live in the default space, and the only thing keeping a query out of
        RETRIEVAL_DOCUMENT space is that its call has exactly one text.
        Batching these -- the obvious speedup, since sending one query per
        office is slow -- would silently move every query into the other
        space and change every similarity score. Nothing would raise; both
        spaces are 3072-dimensional.
        """
        embeddings = []
        for query in queries:
            result = await self._run_in_pool(self.embedding_client.create_embeddings, [query])
            embeddings.append(result[0])
        return embeddings

    async def _build_menu(
        self, br_name: str, state: str, eligible_indices: frozenset[int] | None = None
    ) -> list[DistrictCandidate]:
        """Build the up-to-13-candidate menu for one office.

        FROZEN, owner-decided and not reviewable: top 13 by cosine on the
        race name, then the best hit for the bare "state" query is inserted
        at index 10 (the 11th slot) if it is not already among the race
        results, then the list is truncated back to 13. A heuristic, not a
        guarantee. Do not "fix" it.

        `eligible_indices` narrows WHICH universe indices those
        frozen mechanics may rank -- `None` means every index is eligible,
        today's behavior. It is applied to both rankings before either
        truncates, not after: trimming a denied candidate out of the top 13
        post hoc would still let it occupy a slot and displace an eligible
        rank-14 candidate, and it would still be eligible for the slot-11
        insertion.

        Raises if `state` has no built universe entry. A missing input is
        an infrastructure failure, not a judgment: returning an empty menu
        here would abstain the office and close it on the pending list for
        30 days over a bug, not a model decision.
        """
        state_universe = self._universe_by_state.get(state)
        if state_universe is None:
            raise ValueError(f"No district universe built for state {state!r}; cannot build a menu")

        race_query = f"race name: {br_name}"
        state_query = "state"
        race_query_embedding, state_query_embedding = await self._embed_query_texts([race_query, state_query])

        embeddings = state_universe.embeddings
        pool = range(len(embeddings)) if eligible_indices is None else eligible_indices

        def _candidate(idx: int) -> DistrictCandidate:
            return DistrictCandidate(
                l2_state=state_universe.states[idx],
                l2_district_type=state_universe.district_types[idx],
                l2_district_name=state_universe.district_names[idx],
            )

        race_similarities = sorted(
            ((self._cosine_similarity(race_query_embedding, embeddings[i]), i) for i in pool),
            reverse=True,
        )
        race_results = race_similarities[:MENU_SIZE]
        race_indices = {i for _, i in race_results}
        candidates = [_candidate(idx) for _, idx in race_results]

        state_similarities = sorted(
            ((self._cosine_similarity(state_query_embedding, embeddings[i]), i) for i in pool),
            reverse=True,
        )

        if len(candidates) >= 11 and state_similarities:
            _, state_idx = state_similarities[0]
            if state_idx not in race_indices:
                candidates.insert(STATE_QUERY_INSERT_INDEX, _candidate(state_idx))
                candidates = candidates[:MENU_SIZE]

        if eligible_indices is not None and not candidates:
            # A restriction that empties the menu is cross-repo drift (the
            # universe always carries the synthetic State row today), not a
            # judgment: recording it as an abstention would close the office
            # for 30 days over an infrastructure failure.
            raise ValueError(f"Geography restriction left no eligible candidates for state {state!r}")

        return candidates

    async def _select_candidate(
        self, br_name: str, candidates: list[DistrictCandidate], geography_block: str = ""
    ) -> dict[str, Any]:
        """Ask the LLM to pick a candidate. FROZEN: prompt template,
        response schema, trace name, and the other three variables. Never
        catches -- an LLM failure must fail the run, not be delivered as a
        match.

        `geography_block` is appended to this function's own
        `districts_text` value, after the frozen candidate list -- no new
        template variable. Empty by default so a caller that never
        classifies geography (this file's own frozen-core tests) gets the
        unchanged string.
        """
        district_descriptions = [
            f"{i}. {c.l2_district_name} ({c.l2_district_type})" for i, c in enumerate(candidates, 1)
        ]
        districts_text = "\n".join(district_descriptions)
        if geography_block:
            districts_text = f"{districts_text}\n\n{geography_block}"
        state = candidates[0].l2_state if candidates else "Unknown"
        num_districts = str(len(candidates))

        variables = {
            "br_name": br_name,
            "state": state,
            "districts_text": districts_text,
            "num_districts": num_districts,
        }

        fallback_prompt = f"""
You are analyzing a political position to find the best L2 district match from candidate districts.

BR Position Details:
- Name: "{br_name}"
- State: {state}

Top {num_districts} District Candidates:
{districts_text}

Analyze the BR position and select the BEST matching candidate. Consider:
- Geographic alignment (city/county matching)
- Office type and district type compatibility
- Specific identifiers or numbers in names
- Functional role alignment (e.g., School Board → School Board districts)
- Ignore seats and positions
- if the office is greater than the state level, match to the state level

Return JSON with:
• selected_candidate_number: Number (1-{num_districts}) of your choice, or 0 if no good match
• selection_confidence: Confidence level (0-100)
• reasoning: Detailed explanation of your selection or rejection
• close_alternatives: Array of candidate numbers that were very close (only if multiple options were neck-and-neck)
• is_exact_district_match: Boolean indicating whether the selected candidate matches the BR position at the same specificity level. Set true if the selected L2 candidate is at the same district specificity as the BR position (e.g., "City Council District 3" matched to a district-level L2 entry). Set false if the BR position specifies a sub-district (Ward, District, Place + number) but the selected L2 candidate is a parent-level type (City, County, Township, Borough, Village, Town_District). Set false if selected_candidate_number is 0.

IMPORTANT: Return 0 if no candidate represents a reasonable match.
There is a real probability that the match does not exist so return 0 if there is no clear match.

Base decisions on semantic meaning, geography, and functional appropriateness.
"""

        prompt = build_cached_prompt(self._prompt_name, variables, fallback_prompt=fallback_prompt)
        if not prompt:
            prompt = fallback_prompt

        response_schema = {
            "type": "object",
            "properties": {
                "selected_candidate_number": {"type": "number", "minimum": 0, "maximum": len(candidates)},
                "selection_confidence": {"type": "number", "minimum": 0, "maximum": 100},
                "reasoning": {"type": "string"},
                "close_alternatives": {
                    "type": "array",
                    "items": {"type": "number", "minimum": 0, "maximum": len(candidates)},
                },
                "is_exact_district_match": {"type": "boolean"},
            },
            "required": ["selected_candidate_number", "selection_confidence", "reasoning", "is_exact_district_match"],
        }

        return await self._run_in_pool(
            self.llm.generate_structured_content,
            prompt=prompt,
            response_schema=response_schema,
            trace_name="stitch-match-selection",
        )

    # -- Terminal-outcome contract: a match, an abstention, or a raise ---

    async def match_office(
        self,
        br_database_id: int,
        br_name: str,
        state: str,
        mtfcc: str = "",
        is_judicial: bool = False,
        has_unknown_boundaries: bool = False,
        geo_id: str | None = None,
        sub_area_name: str | None = None,
        sub_area_value: str | None = None,
        school_whole_assertion_enabled: bool = SCHOOL_WHOLE_ASSERTION_ENABLED,
    ) -> MatchResult:
        """Match one BR office to an L2 district, or abstain.

        Persists only matches and abstentions. Never converts an LLM or
        embedding failure into a result -- it propagates so the run fails
        instead of being recorded as a match (the old
        `selected_district_name == "LLM_ERROR"` bug this replaces).

        The geography parameters default to values that classify as
        pass-through (not judicial, no sub_area), so an existing
        caller that only cares about the terminal-outcome contract is
        unaffected. A geography-driven abstain returns before any query
        embedding or LLM call, with `confidence=None` -- no model judgment
        happened.
        """
        state_universe = self._universe_by_state.get(state)
        if state_universe is None:
            raise ValueError(f"No district universe built for state {state!r}; cannot classify or match")

        verdict = _classify_office_geography(
            mtfcc=mtfcc,
            is_judicial=is_judicial,
            has_unknown_boundaries=has_unknown_boundaries,
            geo_id=geo_id,
            sub_area_name=sub_area_name,
            sub_area_value=sub_area_value,
            state_district_types=state_universe.district_types,
            school_whole_assertion_enabled=school_whole_assertion_enabled,
        )
        if verdict.abstain:
            return MatchResult(br_database_id, None, None, None, confidence=None)

        candidates = await self._build_menu(br_name, state, verdict.eligible_indices)
        # No block when no geography was supplied (mtfcc is 100% populated on
        # the real worklist, so an empty one means a caller that never
        # classified): fabricated blank context is worse than none.
        geography_block = (
            _build_geography_block(
                mtfcc, sub_area_name, sub_area_value, has_unknown_boundaries, verdict.verdict_sentence
            )
            if mtfcc
            else ""
        )

        response = await self._select_candidate(br_name, candidates, geography_block)
        selected_index, confidence = _selection_from_response(response, len(candidates))

        if selected_index == 0:
            return MatchResult(br_database_id, None, None, None, confidence=confidence)

        selected = candidates[selected_index - 1]
        return MatchResult(
            br_database_id=br_database_id,
            l2_state=selected.l2_state,
            l2_district_type=selected.l2_district_type,
            l2_district_name=selected.l2_district_name,
            confidence=confidence,
        )

    # -- Orchestration and dry run ---------------------------------------

    async def run(
        self,
        states: list[str] | None = None,
        limit: int | None = None,
        batch_size: int = 100,
        embedding_batch_size: int = 100,
        school_whole_assertion_enabled: bool = SCHOOL_WHOLE_ASSERTION_ENABLED,
    ) -> list[MatchResult]:
        """Match the pending worklist. Writes nothing -- this returns
        terminal results for the caller to print or persist.

        Takes the cost-baseline snapshot here, not at construction. Both
        clients report a LIFETIME cumulative total, so a construction-time
        baseline would make a second run() on the same instance report the
        first run's spend too.
        """
        self._embedding_cost_baseline = self.embedding_client.get_cost_stats()["total_cost"]
        self._llm_cost_baseline = self.llm.get_usage_stats()["total_cost"]

        try:
            pending_df = self.load_pending_offices(states=states, limit=limit)
            if pending_df.empty:
                self.logger.warning("No pending offices matched the given filters")
                return []

            worklist_states = sorted(pending_df["state"].unique())
            self.logger.info(f"Building the district universe for {len(worklist_states)} state(s)")
            await self.build_universe(worklist_states, embedding_batch_size)

            offices = list(pending_df.itertuples(index=False))
            results: list[MatchResult] = []
            for batch_start in range(0, len(offices), batch_size):
                batch = offices[batch_start : batch_start + batch_size]
                batch_results = await asyncio.gather(
                    *(
                        self.match_office(
                            br_database_id=office.br_database_id,
                            br_name=office.name,
                            state=office.state,
                            mtfcc=office.mtfcc,
                            is_judicial=office.is_judicial,
                            has_unknown_boundaries=office.has_unknown_boundaries,
                            geo_id=office.geo_id,
                            sub_area_name=office.sub_area_name,
                            sub_area_value=office.sub_area_value,
                            school_whole_assertion_enabled=school_whole_assertion_enabled,
                        )
                        for office in batch
                    )
                )
                results.extend(batch_results)
                self.logger.info(f"Matched {len(results)}/{len(offices)} offices")

            return results
        except Exception:
            embedding_cost, llm_cost = self._cost_deltas()
            self.logger.error(
                f"Run failed. Partial cost before failure -- embedding: ${embedding_cost:.6f}, "
                f"LLM: ${llm_cost:.6f}, total: ${embedding_cost + llm_cost:.6f}"
            )
            raise

    def _cost_deltas(self) -> tuple[float, float]:
        """This run's (embedding_cost, llm_cost). Both clients report a
        lifetime cumulative total, so this is always a delta against the
        baseline `run()` takes at its own start.
        """
        embedding_cost = self.embedding_client.get_cost_stats()["total_cost"] - self._embedding_cost_baseline
        llm_cost = self.llm.get_usage_stats()["total_cost"] - self._llm_cost_baseline
        return embedding_cost, llm_cost

    def print_summary(self, results: list[MatchResult]) -> None:
        """Print the match/abstain split and this run's cost.

        A populated district name is what says the office matched -- there
        is no status column to count, here or in the results table.
        """
        matched = sum(1 for r in results if r.l2_district_name is not None)
        embedding_cost, llm_cost = self._cost_deltas()

        self.logger.info(f"Processed {len(results)} offices")
        self.logger.info(f"  matched: {matched}")
        self.logger.info(f"  abstained: {len(results) - matched}")
        self.logger.info(f"Embedding cost: ${embedding_cost:.6f}")
        self.logger.info(f"LLM cost: ${llm_cost:.6f}")
        self.logger.info(f"Total cost: ${embedding_cost + llm_cost:.6f}")

    # -- Resource lifecycle ----------------------------------------------

    def close(self) -> None:
        """Release the Databricks connection. `main()` is the only caller.

        There is deliberately no `self._executor.shutdown(...)` here, and
        that is worth stating because it looks like an omission. A failed run
        leaves up to a batch of Gemini calls mid-retry in non-daemon threads,
        and `concurrent.futures.thread`'s atexit hook joins every live worker
        -- so the CLI does sit there for as long as their own backoff takes,
        up to roughly 1,023s at max_retries=11, after the traceback has
        printed. `shutdown(wait=False, cancel_futures=True)` does NOT fix it:
        it deregisters nothing from `_threads_queues`, so the atexit join
        happens either way, and `cancel_futures` drains only QUEUED work,
        which is always empty here (run() awaits each group of `batch_size`
        offices, ~100 in flight against a 1,500-worker pool, so nothing ever
        queues). Measured both ways: identical exit time.

        The real fix is for the clients in shared/ to stop retrying blindly,
        which is where the quota work goes too.
        """
        self.databricks.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Match BallotReady offices to L2 districts. Dry run: writes nothing.")
    parser.add_argument(
        "--states", nargs="+", type=_canonical_state_arg, help="Limit to these state codes (e.g. --states DE CA)"
    )
    parser.add_argument("--limit", type=_positive_int, help="Limit the number of pending offices read (positive)")
    parser.add_argument(
        "--batch-size",
        type=_positive_int,
        default=100,
        help="Offices matched concurrently per group (positive; default: 100)",
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=_positive_int,
        default=100,
        help="District texts embedded per call when building the universe (positive; default: 100)",
    )
    parser.add_argument(
        "--enable-school-whole-assertion",
        action="store_true",
        help=(
            "Deny school SUB-level types for whole-asserted school offices. Off by default until "
            "the holdout adjudicates residency zones vs zoned electorates; the holdout runs both arms"
        ),
    )
    return parser.parse_args(argv)


async def main() -> None:
    args = _parse_args()
    matcher = L2BrMatcher()
    try:
        results = await matcher.run(
            states=args.states,
            limit=args.limit,
            batch_size=args.batch_size,
            embedding_batch_size=args.embedding_batch_size,
            school_whole_assertion_enabled=args.enable_school_whole_assertion,
        )
        matcher.print_summary(results)
    finally:
        # Ordered and guarded, because close() ends in Connection.close(),
        # which raises on exactly the dead-session conditions that failed the
        # run. flush_logs() runs first so a teardown failure cannot lose the
        # Braintrust buffer, and the close is swallowed so it cannot replace
        # the operator's real traceback with an unrelated cleanup error --
        # which reordering alone does NOT prevent, since a raise here still
        # supersedes the exception in flight.
        flush_logs()
        try:
            matcher.close()
        except Exception:
            matcher.logger.warning("matcher.close() raised during teardown", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
