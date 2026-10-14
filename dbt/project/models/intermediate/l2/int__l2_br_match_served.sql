-- The matcher's latest attempt per office, with a match withheld (published as
-- unmatched) when it fits a wrong-answer shape the run audits kept finding. Abstain
-- only: a guard never picks a different district, and an override in
-- l2_br_match_overrides still wins in every consumer. The pending selector reads the
-- results table directly, so a withheld office is not re-matched by this.
with
    matches as (select * from {{ ref("stg_model_predictions__llm_l2_br_match") }}),

    positions as (
        select
            database_id as br_database_id,
            name as office_name,
            state as office_state,
            coalesce(sub_area_name, '') as sub_area_name,
            coalesce(sub_area_value, '') as sub_area_value
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    shaped as (
        select
            matches.*,
            positions.office_name,
            positions.office_state,
            positions.sub_area_name,
            positions.sub_area_value,
            -- "East Hanover Township" -> "EAST HANOVER": the member town a board seat
            -- is apportioned to, when BallotReady names one as the sub-area.
            upper(
                trim(
                    regexp_replace(
                        positions.sub_area_value,
                        '(?i)\\s*\\b(township|borough|town|village|city)$',
                        ''
                    )
                )
            ) as member_town,
            upper(split(positions.office_name, ' - ')[0]) as board_name
        from matches
        left join positions on matches.br_database_id = positions.br_database_id
    ),

    guarded as (
        select
            *,
            case
                when
                    not is_matched
                    or attempted_at < timestamp('{{ var("l2_br_match_guards_from") }}')
                then null
                -- Special districts L2 rarely carries land on a same-named city,
                -- town, county, census place, or the whole state. A county-named
                -- district (Rawlins County Hospital District) can be its county.
                when
                    office_name
                    rlike '(?i)community development district|improvement district|utility (development )?district|\\bmud\\b|water control district|drainage district|hospital (district|authority)|fire rescue district|fire protection district|common facilities district|special (taxing )?district|recreation district|stewardship district'
                    and l2_district_type in (
                        'State',
                        'County',
                        'City',
                        'Town',
                        'Town_District',
                        'Township',
                        'Village',
                        'Borough',
                        'Hamlet_Community_Area'
                    )
                    and not (
                        l2_district_type = 'County'
                        and instr(
                            upper(office_name), upper(l2_district_name) || ' COUNTY'
                        )
                        > 0
                    )
                then 'special_district_on_general_row'
                -- A seat apportioned to one member town, matched to the parent body.
                -- Only where by-town election is verified: Indiana-style boards elect
                -- these seats at large. A row named for the town itself (NATIONAL PARK
                -- ELEM SD) is that town's electorate unless the board shares the name.
                when
                    office_state in ('NJ', 'AR', 'MA')
                    and sub_area_name = ''
                    and sub_area_value
                    rlike '(?i)\\b(township|borough|town|village|city)$'
                    and l2_district_type not in (
                        'Township',
                        'Borough',
                        'Town',
                        'Town_District',
                        'Village',
                        'City'
                    )
                    -- A subdistrict row is a zone of the board, not the parent body.
                    and not l2_district_type
                    rlike '(?i)subdistrict|school_board_district|college_board_district'
                    and not (
                        instr(upper(l2_district_name), member_town) > 0
                        and instr(board_name, member_town) = 0
                    )
                then 'town_seat_on_parent_body'
                -- L2's county-numbered NJ fire rows pool every town's district of that
                -- number in most counties. Camden numbers uniquely, a known miss.
                when
                    office_state = 'NJ'
                    and l2_district_type = 'Fire_District'
                    and l2_district_name like '% CNTY FD%'
                    and office_name rlike '(?i)\\b(township|borough|city|town)\\b'
                then 'township_fire_on_county_pool'
                when
                    office_name rlike '(?i)\\bcounty (court )?judge\\b'
                    and l2_district_type
                    not in ('County', 'Judicial_County_Court_District')
                then 'county_judge_off_county'
                when
                    office_name rlike '(?i)\\bcollege\\b'
                    and not office_name rlike '(?i)school board'
                    and l2_district_type in (
                        'School_District',
                        'Unified_School_District',
                        'County_Unified_School_District',
                        'Elementary_School_District',
                        'Middle_School_District',
                        'High_School_District',
                        'City_School_District',
                        'Exempted_Village_School_District'
                    )
                then 'college_on_k12_row'
                -- A census-designated place has no elected government.
                when l2_district_type = 'Hamlet_Community_Area'
                then 'census_place'
            end as withheld_reason
        from shaped
    )

select
    br_database_id,
    if(withheld_reason is null, l2_state, null) as l2_state,
    if(withheld_reason is null, l2_district_type, null) as l2_district_type,
    if(withheld_reason is null, l2_district_name, null) as l2_district_name,
    confidence,
    attempted_at,
    is_matched and withheld_reason is null as is_matched,
    withheld_reason
from guarded
