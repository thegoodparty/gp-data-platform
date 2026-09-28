-- Person-grain HubSpot crosswalk for the Serve product. Passed through to this
-- mart so the Serve credential resolves a contact id without read access to the
-- civics mart; the identifier columns only, since the person mart carries PII
-- this mart deliberately excludes.
select gp_person_id, hs_contact_id from {{ ref("people") }}
