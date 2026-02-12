{#
  Normalizes state names/abbreviations to 2-letter USPS abbreviation.
  Accepts either full state names (e.g. 'California') or abbreviations (e.g. 'CA').
  Usage: {{ normalize_state_to_abbr('state_column') }}
#}
{% macro normalize_state_to_abbr(column_name) %}
    case
        when upper(trim({{ column_name }})) in ('AL', 'ALABAMA')
        then 'AL'
        when upper(trim({{ column_name }})) in ('AK', 'ALASKA')
        then 'AK'
        when upper(trim({{ column_name }})) in ('AZ', 'ARIZONA')
        then 'AZ'
        when upper(trim({{ column_name }})) in ('AR', 'ARKANSAS')
        then 'AR'
        when upper(trim({{ column_name }})) in ('CA', 'CALIFORNIA')
        then 'CA'
        when upper(trim({{ column_name }})) in ('CO', 'COLORADO')
        then 'CO'
        when upper(trim({{ column_name }})) in ('CT', 'CONNECTICUT')
        then 'CT'
        when upper(trim({{ column_name }})) in ('DE', 'DELAWARE')
        then 'DE'
        when upper(trim({{ column_name }})) in ('DC', 'DISTRICT OF COLUMBIA')
        then 'DC'
        when upper(trim({{ column_name }})) in ('FL', 'FLORIDA')
        then 'FL'
        when upper(trim({{ column_name }})) in ('GA', 'GEORGIA')
        then 'GA'
        when upper(trim({{ column_name }})) in ('HI', 'HAWAII')
        then 'HI'
        when upper(trim({{ column_name }})) in ('ID', 'IDAHO')
        then 'ID'
        when upper(trim({{ column_name }})) in ('IL', 'ILLINOIS')
        then 'IL'
        when upper(trim({{ column_name }})) in ('IN', 'INDIANA')
        then 'IN'
        when upper(trim({{ column_name }})) in ('IA', 'IOWA')
        then 'IA'
        when upper(trim({{ column_name }})) in ('KS', 'KANSAS')
        then 'KS'
        when upper(trim({{ column_name }})) in ('KY', 'KENTUCKY')
        then 'KY'
        when upper(trim({{ column_name }})) in ('LA', 'LOUISIANA')
        then 'LA'
        when upper(trim({{ column_name }})) in ('ME', 'MAINE')
        then 'ME'
        when upper(trim({{ column_name }})) in ('MD', 'MARYLAND')
        then 'MD'
        when upper(trim({{ column_name }})) in ('MA', 'MASSACHUSETTS')
        then 'MA'
        when upper(trim({{ column_name }})) in ('MI', 'MICHIGAN')
        then 'MI'
        when upper(trim({{ column_name }})) in ('MN', 'MINNESOTA')
        then 'MN'
        when upper(trim({{ column_name }})) in ('MS', 'MISSISSIPPI')
        then 'MS'
        when upper(trim({{ column_name }})) in ('MO', 'MISSOURI')
        then 'MO'
        when upper(trim({{ column_name }})) in ('MT', 'MONTANA')
        then 'MT'
        when upper(trim({{ column_name }})) in ('NE', 'NEBRASKA')
        then 'NE'
        when upper(trim({{ column_name }})) in ('NV', 'NEVADA')
        then 'NV'
        when upper(trim({{ column_name }})) in ('NH', 'NEW HAMPSHIRE')
        then 'NH'
        when upper(trim({{ column_name }})) in ('NJ', 'NEW JERSEY')
        then 'NJ'
        when upper(trim({{ column_name }})) in ('NM', 'NEW MEXICO')
        then 'NM'
        when upper(trim({{ column_name }})) in ('NY', 'NEW YORK')
        then 'NY'
        when upper(trim({{ column_name }})) in ('NC', 'NORTH CAROLINA')
        then 'NC'
        when upper(trim({{ column_name }})) in ('ND', 'NORTH DAKOTA')
        then 'ND'
        when upper(trim({{ column_name }})) in ('OH', 'OHIO')
        then 'OH'
        when upper(trim({{ column_name }})) in ('OK', 'OKLAHOMA')
        then 'OK'
        when upper(trim({{ column_name }})) in ('OR', 'OREGON')
        then 'OR'
        when upper(trim({{ column_name }})) in ('PA', 'PENNSYLVANIA')
        then 'PA'
        when upper(trim({{ column_name }})) in ('RI', 'RHODE ISLAND')
        then 'RI'
        when upper(trim({{ column_name }})) in ('SC', 'SOUTH CAROLINA')
        then 'SC'
        when upper(trim({{ column_name }})) in ('SD', 'SOUTH DAKOTA')
        then 'SD'
        when upper(trim({{ column_name }})) in ('TN', 'TENNESSEE')
        then 'TN'
        when upper(trim({{ column_name }})) in ('TX', 'TEXAS')
        then 'TX'
        when upper(trim({{ column_name }})) in ('UT', 'UTAH')
        then 'UT'
        when upper(trim({{ column_name }})) in ('VT', 'VERMONT')
        then 'VT'
        when upper(trim({{ column_name }})) in ('VA', 'VIRGINIA')
        then 'VA'
        when upper(trim({{ column_name }})) in ('WA', 'WASHINGTON')
        then 'WA'
        when upper(trim({{ column_name }})) in ('WV', 'WEST VIRGINIA')
        then 'WV'
        when upper(trim({{ column_name }})) in ('WI', 'WISCONSIN')
        then 'WI'
        when upper(trim({{ column_name }})) in ('WY', 'WYOMING')
        then 'WY'
        else upper(trim({{ column_name }}))
    end
{% endmacro %}
