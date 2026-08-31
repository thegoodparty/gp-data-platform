-- A suppression row exists because someone reported a photo that is not the
-- person, so a surviving headshot_url is a privacy failure rather than a
-- cosmetic one. Guards the override join against a later refactor of the mart.
select ovr.br_person_id
from {{ ref("election_api_person_image_overrides") }} as ovr
inner join
    {{ ref("m_election_api__person") }} as person
    on ovr.br_person_id = person.br_person_id
where ovr.suppress_image and person.headshot_url is not null
