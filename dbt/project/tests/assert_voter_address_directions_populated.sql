-- The four address direction columns hold N/S/E/W, so any numeric cast on them
-- nulls the whole column nationwide. That failure is invisible downstream: the
-- address line still renders, just without its direction, and in grid-addressed
-- cities the direction is most of the address. Shares run 6% to 20%, so a 1%
-- floor only fires on a wipe.
{% set min_share = 0.01 %}

with
    populated as (
        select
            count(*) as total_rows,
            count(`Residence_Addresses_PrefixDirection`) as residence_prefix,
            count(`Residence_Addresses_SuffixDirection`) as residence_suffix,
            count(`Mailing_Addresses_PrefixDirection`) as mailing_prefix,
            count(`Mailing_Addresses_SuffixDirection`) as mailing_suffix
        from {{ ref("m_people_api__voter") }}
    ),
    per_column as (
        select
            'Residence_Addresses_PrefixDirection' as column_name,
            residence_prefix as populated_rows,
            total_rows
        from populated
        union all
        select 'Residence_Addresses_SuffixDirection', residence_suffix, total_rows
        from populated
        union all
        select 'Mailing_Addresses_PrefixDirection', mailing_prefix, total_rows
        from populated
        union all
        select 'Mailing_Addresses_SuffixDirection', mailing_suffix, total_rows
        from populated
    )
select column_name, populated_rows, total_rows
from per_column
where populated_rows < total_rows * {{ min_share }}
