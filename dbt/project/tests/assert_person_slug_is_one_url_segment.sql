-- A person slug fills a single /people/<slug> path segment, so it must not
-- contain '/'. slugify keeps '/' by default because place and race slugs nest,
-- which is how junk in a name field (a literal 'n/a', a date of birth, a dual
-- 'first/second' name) produced profiles that could never route.
select id, slug from {{ ref("m_election_api__person") }} where slug like '%/%'
