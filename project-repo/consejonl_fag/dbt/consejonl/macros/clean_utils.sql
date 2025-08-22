{% macro clean_text(col) -%}
    nullif(trim({{ col }}), '')
{%- endmacro %}

{% macro clean_numeric(col) -%}
    -- Trata '', 'NULL', 'NaN' como NULL, elimina símbolos, maneja paréntesis (negativos contables)
    iff(
        regexp_like({{ col }}, '^\s*\(.*\)\s*$'),
        -1, 1
    ) * try_to_number(
          nullif(
            regexp_replace(
              iff(upper(trim({{ col }})) in ('', 'NULL', 'NAN'), null, trim({{ col }})),
              '[^0-9\.\-]',
              ''
            ),
            ''
          )
        )
{%- endmacro %}

{% macro clean_date(col) -%}
    /* Robust date parser: tries multiple formats without raising errors */
    coalesce(
      try_to_date(nullif(trim({{ col }}), '')),
      try_to_date(nullif(trim({{ col }}), ''), 'YYYY-MM-DD'),
      try_to_date(nullif(trim({{ col }}), ''), 'YYYY/MM/DD'),
      try_to_date(nullif(trim({{ col }}), ''), 'DD/MM/YYYY'),
      try_to_date(nullif(trim({{ col }}), ''), 'MM/DD/YYYY'),
      try_to_date(nullif(trim({{ col }}), ''), 'YYYY-MM-DD HH24:MI:SS'),
      try_to_date(nullif(trim({{ col }}), ''), 'YYYY/MM/DD HH24:MI:SS')
    )
{%- endmacro %}
