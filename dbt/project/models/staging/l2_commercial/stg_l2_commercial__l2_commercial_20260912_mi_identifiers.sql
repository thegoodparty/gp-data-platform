{%- set columns = [
    "Individual_Id",
    "Address_Id",
    "Family_Id",
    "Lalvoterid",
    "Fname",
    "Mname",
    "Lname",
    "Name_Suffix",
    "House_Number",
    "Pre_Direction",
    "Street_Name",
    "Street_Suffix",
    "Post_Direction",
    "Unit_Designator_Number",
    "Primary_Address",
    "Secondary_Address",
    "Delivery_Point_Check_Digit",
    "Landline_Phone_Number",
    "Cell_Phone",
    "Latitude",
    "Longitude",
] -%}

select
    sha2(nullif(trim(`Individual_Id`), ''), 256) as individual_id_hash,
    {%- for name in columns %}
        nullif(trim(`{{ name }}`), '') as {{ name | lower }}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ source("l2_commercial", "l2_commercial_20260912_mi") }}
