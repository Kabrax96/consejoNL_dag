-- macros/generate_schema_name.sql

{% macro generate_schema_name(custom_schema_name, node) -%}
  {#
    Comportamiento:
    1) Si el modelo tiene schema explícito (custom_schema_name), se usa como base.
    2) Si no, se usa target.schema.
    3) Si var('is_cp', false) es true, se agrega un sufijo (por defecto "_cp").
       - Para cambiar el sufijo: --vars '{"cp_schema_suffix": "_cp_anual"}'
    4) (Opcional) También puedes activar CP por tag:
       --vars '{"cp_tags": ["balance_presupuestario_cp","cp"]}'
       Si el modelo tiene alguna de esas tags, se considera "CP".
    5) Evita duplicar el sufijo si ya está aplicado.
  #}

  {%- set base_schema = (custom_schema_name | trim) if custom_schema_name is not none else target.schema -%}

  {# Variables de control #}
  {%- set is_cp_var = var('is_cp', false) -%}
  {%- set cp_suffix = var('cp_schema_suffix', '_cp') -%}
  {%- set cp_tags = var('cp_tags', []) -%} {# lista opcional de tags que fuerzan CP #}

  {# ¿El nodo tiene alguna de las tags que activan CP? #}
  {%- set node_tags = node.config.tags | default([]) -%}
  {%- set has_cp_tag = (node_tags | select('in', cp_tags) | list | length) > 0 -%}

  {# ¿Debemos aplicar modo CP? Prioridad: var('is_cp') OR tag incluida en cp_tags #}
  {%- set use_cp = is_cp_var or has_cp_tag -%}

  {# Evita doble sufijo si el schema ya termina con cp_suffix #}
  {%- if use_cp and not base_schema.endswith(cp_suffix) -%}
    {{ base_schema ~ cp_suffix }}
  {%- else -%}
    {{ base_schema }}
  {%- endif -%}
{%- endmacro %}
