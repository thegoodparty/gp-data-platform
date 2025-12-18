-- Generic test to validate that all labels in a bucket array contain only accepted
-- values
-- The bucket is a nested array of structs with {label, count, percent} structure
{% test bucket_labels_accepted_values(model, bucket_path, accepted_values) %}

    {%- set values_list = accepted_values | join("', '") -%}

    with
        extracted_array as (
            select district_id, {{ bucket_path }} as bucket_array from {{ model }}
        ),

        exploded_labels as (
            select district_id, bucket_entry.label as label
            from extracted_array
            lateral view explode(bucket_array) as bucket_entry
        ),

        invalid_labels as (
            select distinct district_id, label
            from exploded_labels
            where label not in ('{{ values_list }}')
        )

    select *
    from invalid_labels

{% endtest %}
