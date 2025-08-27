{% macro remove_techspeed_name_suffixes(last_name_column) %}
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                regexp_replace(
                                                    regexp_replace(
                                                        {{ last_name_column }},
                                                        ' Jr$',
                                                        ''
                                                    ),
                                                    ' Jr\.$',
                                                    ''
                                                ),
                                                ' Sr$',
                                                ''
                                            ),
                                            ' Sr\.$',
                                            ''
                                        ),
                                        ' II$',
                                        ''
                                    ),
                                    ' Ii$',
                                    ''
                                ),
                                ' III$',
                                ''
                            ),
                            ' Iii$',
                            ''
                        ),
                        ' Iv$',
                        ''
                    ),
                    ' I ii$',
                    ''
                ),
                ' V$',
                ''
            ),
            ' Iiii$',
            ''
        )
    )
{% endmacro %}
