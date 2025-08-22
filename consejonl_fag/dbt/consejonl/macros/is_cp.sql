-- consejonl_fag/dbt/consejonl/macros/is_cp.sql
{% macro is_cp() %}
  {{ return(var('is_cp', false)) }}
{% endmacro %}
