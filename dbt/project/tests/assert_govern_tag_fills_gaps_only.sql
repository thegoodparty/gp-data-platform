-- The Govern product tag may only place events the name rules leave in 'other'.
select event_type, name_rule_family, family
from {{ ref("int__amplitude_event_catalog") }}
where family != name_rule_family and name_rule_family != 'other'
