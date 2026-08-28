-- Every race slug must extend its resolved place's slug: the race mart
-- derives slug as <place slug>/<position slug> from m_election_api__place,
-- and election-api routes race pages under the place page. A failing row
-- means the two marts disagree about a place's canonical slug (e.g. a race
-- slug still built from a raw, un-disambiguated place slug).
select tbl_race.id, tbl_race.slug as race_slug, tbl_place.slug as place_slug
from {{ ref("m_election_api__race") }} as tbl_race
inner join
    {{ ref("m_election_api__place") }} as tbl_place on tbl_race.place_id = tbl_place.id
where not startswith(tbl_race.slug, concat(tbl_place.slug, '/'))
