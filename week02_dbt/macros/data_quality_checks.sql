{# Data Quality Check Macros #}

{# Outputs a JSON object with NULL check result for each row. check_fail is 1 if NULL, 0 otherwise. #}
{% macro check_null(column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'NULL_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF({{ column_name }} IS NULL, 1, 0)
    )
{% endmacro %}

{# Outputs a JSON object with duplicate check result. check_fail is 1 if value appears more than once. #}
{% macro check_unique(column_name, count_column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'DUPLICATE_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF({{ count_column_name }} > 1, 1, 0)
    )
{% endmacro %}

{# Outputs a JSON object with positive value check. check_fail is 1 if value is zero or negative. #}
{% macro check_positive(column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'POSITIVE_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF({{ column_name }} <= 0, 1, 0)
    )
{% endmacro %}

{# Outputs a JSON object with age validation. check_fail is 1 if age is outside 0-120 range. #}
{% macro check_valid_age(column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'VALID_AGE_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF({{ column_name }} < 0 OR {{ column_name }} > 120, 1, 0)
    )
{% endmacro %}

{# Outputs a JSON object with date validation. check_fail is 1 if date is future or before 1900. #}
{% macro check_valid_date(column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'VALID_DATE_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF({{ column_name }} > CURRENT_DATE() OR {{ column_name }} < '1900-01-01', 1, 0)
    )
{% endmacro %}

{# Outputs a JSON object with ZIP code validation. check_fail is 1 if not a valid 5-digit ZIP. #}
{% macro check_valid_zip(column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'VALID_ZIP_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF({{ column_name }} < 10000 OR {{ column_name }} > 99999, 1, 0)
    )
{% endmacro %}

{# Outputs a JSON object with phone validation. check_fail is 1 if phone doesn't have exactly 10 digits. #}
{% macro check_valid_phone(column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'VALID_PHONE_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF(LENGTH(REGEXP_REPLACE({{ column_name }}, '[^0-9]', '')) != 10, 1, 0)
    )
{% endmacro %}

{# Outputs a JSON object with state validation. check_fail is 1 if not a valid US state code. #}
{% macro check_valid_state(column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'VALID_STATE_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF(UPPER({{ column_name }}) NOT IN (
            'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
            'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
            'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
            'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
            'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
            'DC','PR','VI','GU','AS','MP'
        ), 1, 0)
    )
{% endmacro %}

{# Outputs a JSON object with empty string check. check_fail is 1 if value is empty or whitespace only. #}
{% macro check_not_empty(column_name) %}
    OBJECT_CONSTRUCT(
        'column_name', '{{ column_name }}',
        'check_type', 'EMPTY_STRING_CHECK',
        'original_value', {{ column_name }},
        'check_fail', IFF(TRIM({{ column_name }}) = '', 1, 0)
    )
{% endmacro %}
