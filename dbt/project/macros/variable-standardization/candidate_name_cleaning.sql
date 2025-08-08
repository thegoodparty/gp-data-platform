{#
  This macro handles candidate name cleaning operations
  Usage: {{ clean_candidate_names('last_name') }}
#}
{% macro clean_candidate_names(last_name_column) %}
    -- Simple but effective name cleaning that mirrors the original logic
    trim(
        case
            -- Handle compound names with prefixes that should be kept
            when {{ last_name_column }} like '%De La %'
            then
                'De La ' || substring(
                    {{ last_name_column }},
                    position('De La ' in {{ last_name_column }}) + 7
                )
            when {{ last_name_column }} like '%Van %'
            then
                'Van ' || substring(
                    {{ last_name_column }},
                    position('Van ' in {{ last_name_column }}) + 4
                )
            when {{ last_name_column }} like '% van %'
            then
                'van ' || substring(
                    {{ last_name_column }},
                    position(' van ' in {{ last_name_column }}) + 5
                )
            when {{ last_name_column }} like '%Le %'
            then
                'Le ' || substring(
                    {{ last_name_column }},
                    position('Le ' in {{ last_name_column }}) + 3
                )

            -- Handle misplaced middle initials (name ending with space + single letter)
            when
                length({{ last_name_column }}) >= 2
                and substring(
                    {{ last_name_column }}, length({{ last_name_column }}) - 1, 1
                )
                = ' '
            then
                substring({{ last_name_column }}, 1, length({{ last_name_column }}) - 2)

            -- Handle middle initial with period but no space (e.g., "SmithJ." ->
            -- "Smith")
            when
                length({{ last_name_column }}) >= 3
                and substring(
                    {{ last_name_column }}, length({{ last_name_column }}) - 1, 1
                )
                = '.'
                and substring(
                    {{ last_name_column }}, length({{ last_name_column }}) - 2, 1
                )
                != ' '
            then
                substring({{ last_name_column }}, 1, length({{ last_name_column }}) - 2)

            -- Default case: clean suffixes and commas, then take last word if compound
            else
                case
                    when {{ last_name_column }} like '% %'
                    then
                        -- Take everything after the last space for compound names
                        substring(
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
                                                                            ' Jr,',
                                                                            ''
                                                                        ),
                                                                        ' Jr\\.',
                                                                        ''
                                                                    ),
                                                                    ' Sr,',
                                                                    ''
                                                                ),
                                                                ' Sr\\.',
                                                                ''
                                                            ),
                                                            ' II,',
                                                            ''
                                                        ),
                                                        ' Ii,',
                                                        ''
                                                    ),
                                                    ' III,',
                                                    ''
                                                ),
                                                ' Iii,',
                                                ''
                                            ),
                                            ' Iv,',
                                            ''
                                        ),
                                        ' V,',
                                        ''
                                    ),
                                    ',,',
                                    ''
                                ),
                                ',',
                                ''
                            ),
                            length({{ last_name_column }})
                            - position(' ' in reverse({{ last_name_column }}))
                            + 2
                        )
                    else
                        -- Single word: just clean suffixes and commas
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
                                                                        ' Jr,',
                                                                        ''
                                                                    ),
                                                                    ' Jr\\.',
                                                                    ''
                                                                ),
                                                                ' Sr,',
                                                                ''
                                                            ),
                                                            ' Sr\\.',
                                                            ''
                                                        ),
                                                        ' II,',
                                                        ''
                                                    ),
                                                    ' Ii,',
                                                    ''
                                                ),
                                                ' III,',
                                                ''
                                            ),
                                            ' Iii,',
                                            ''
                                        ),
                                        ' Iv,',
                                        ''
                                    ),
                                    ' V,',
                                    ''
                                ),
                                ',,',
                                ''
                            ),
                            ',',
                            ''
                        )
                end
        end
    )
{% endmacro %}
