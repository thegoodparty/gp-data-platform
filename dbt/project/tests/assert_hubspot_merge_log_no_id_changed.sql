-- A merge that returns an id other than the primary breaks the assumption the
-- survivors' ranking and the queue are built on: the contact everyone downstream
-- treats as the survivor is gone. The queue empties itself while such a row is
-- unreviewed, so this test is what tells a person to look.
select run_id, primary_id, secondary_id, survivor_id, attempted_at
from {{ ref("stg_airflow_source__hubspot_contact_merge_log") }}
where status = 'id_changed'
