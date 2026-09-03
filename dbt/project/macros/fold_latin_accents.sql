{% macro fold_latin_accents(expression) %}
    {#-
        Fold Latin letters carrying diacritics onto their ASCII base, so a
        caller's strip step keeps the letter instead of deleting it: 'josé'
        folds to 'jose' rather than being cut to 'jos'. Expects an
        already-lowercased expression.

        Only Latin script is folded. Scripts that write no short vowels (Arabic,
        Hebrew) transliterate to a consonant skeleton — 'محمد' gives 'mhmd' —
        which reads as gibberish rather than a name, so they are left to the
        strip step and slug from their id suffix alone. Cyrillic and CJK are
        likewise untouched: a correct romanization is language-specific and
        belongs in the source data, not in a slug macro.

        The character set is every lowercase Latin letter in Latin-1
        Supplement, Latin Extended-A/B and Latin Extended Additional that NFD
        decomposes to an ASCII base plus combining marks, together with the
        letters whose diacritic is part of the glyph and so does not decompose
        (ø, ł, đ, ħ, ŧ, ı). It agrees with unidecode on every character.

        The two translate() arguments are generated from one map, so they cannot
        drift out of equal length.
    -#}
    {%- set multi_char = {
        "ß": "ss",
        "æ": "ae",
        "œ": "oe",
        "þ": "th",
        "ĳ": "ij",
    } -%}
    {%- set single_char = {
        "a": "àáâãäåāăąǎǟǡǻḁạảấầẩẫậắằẳẵặ",
        "b": "ḃḅḇ",
        "c": "çćĉċčḉ",
        "d": "ðďđḋḍḏḑḓ",
        "e": "èéêëēĕėęěḕḗḙḛḝẹẻẽếềểễệ",
        "f": "ḟ",
        "g": "ĝğġģǧǵḡ",
        "h": "ĥħḣḥḧḩḫẖ",
        "i": "ìíîïĩīĭįıǐḭḯỉị",
        "j": "ĵǰ",
        "k": "ķĸǩḱḳḵ",
        "l": "ĺļľŀłḷḹḻḽ",
        "m": "ḿṁṃ",
        "n": "ñńņňǹṅṇṉṋ",
        "o": "òóôõöøōŏőơǒǫǭṍṏṑṓọỏốồổỗộớờởỡợ",
        "p": "ṕṗ",
        "r": "ŕŗřṙṛṝṟ",
        "s": "śŝşšſṡṣṥṧṩ",
        "t": "ţťŧṫṭṯṱẗ",
        "u": "ùúûüũūŭůűųưǔǖǘǚǜṳṵṷṹṻụủứừửữự",
        "v": "ṽṿ",
        "w": "ŵẁẃẅẇẉẘ",
        "x": "ẋẍ",
        "y": "ýÿŷẏẙỳỵỷỹ",
        "z": "źżžẑẓẕ",
    } -%}
    {%- set from_chars = [] -%}
    {%- set to_chars = [] -%}
    {%- for base, accented in single_char.items() -%}
        {%- do from_chars.append(accented) -%}
        {%- do to_chars.append(base * (accented | length)) -%}
    {%- endfor -%}
    {#- Multi-character expansions cannot go through translate(), which is 1:1. -#}
    {%- set ns = namespace(expr=expression) -%}
    {%- for accented, ascii_form in multi_char.items() -%}
        {%- set ns.expr = (
            "regexp_replace("
            ~ ns.expr
            ~ ", '"
            ~ accented
            ~ "', '"
            ~ ascii_form
            ~ "')"
        ) -%}
    {%- endfor -%}
    translate({{ ns.expr }}, '{{ from_chars | join("") }}', '{{ to_chars | join("") }}')
{% endmacro %}
