{% macro generate_schema(custom_schema_name, node) %}
  {%- set default_schema = target.schema -%}

  {%- if custom_schema_name is none -%}
    {{ return(default_schema) }}
  {%- else -%}
    {{ return(custom_schema_name | trim) }}
  {%- endif -%}
{% endmacro %}
