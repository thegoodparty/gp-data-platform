{{ config(materialized="sql") }}

-- Pinned exactly: dependencies install fresh on each execution, so an unpinned
-- release reaches prod untested (3.0 removed HumanName), and parsed names feed ER.
create or replace function {{ this }} (full_name string)
returns
    struct<
        title:string,
        first:string,
        middle:string,
        last:string,
        suffix:string,
        nickname:string
    >
language
    python environment(
        dependencies = '["nameparser==2.8.2"]', environment_version = 'None'
    )
as $$
if not full_name:
    return None
from nameparser import HumanName
name = HumanName(full_name)
return {
    "title": name.title or None,
    "first": name.first or None,
    "middle": name.middle or None,
    "last": name.last or None,
    "suffix": name.suffix or None,
    "nickname": name.nickname or None,
}
$$
