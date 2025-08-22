/*
===========================================================================================================
Model Name          : t3__finanzas__egresos_no_etiquetados
Author              : Alejandro Morales Benavides
Date                : August 12th, 2025
Usage               : dbt build --select t3__finanzas__egresos_no_etiquetados

Objective           :
    This model:
        1) Filters detailed expenditures to Section I (Gasto No Etiquetado).
        2) Exposes a clean, business-ready slice for downstream marts and indicators.

Dependencies        :
    - Source model: t2__finanzas__egresos_detallado

Assumptions/Notes   :
    - Records are filtered where SECCION = 'I'.
    - Uses columns as defined in Tier 2 (no renaming here).

===========================================================================================================
History             :
-----------------------------------------------------------------------------------------------------------
Name                          | Date        | Project     | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides   | 2025-08-12  | consejonl   | Created Tier 3 model for Section I (Gasto No Etiquetado).
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}

{{
    config(
        materialized = "table",
        snowflake_warehouse = "COMPUTE_WH",
        pre_hook = ["SET start_time = TO_TIMESTAMP('2000-01-01'); SET end_time = CURRENT_TIMESTAMP;"],
        post_hook = [
            "{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"
        ]
    )
}}

with base as (
  select
    fecha,
    codigo,
    cuarto,
    pagado,
    seccion,
    aprobado,
    concepto,
    devengado,
    modificado,
    subejercicio,
    surrogate_key,
    "AMPLIACIONES/REDUCCIONES",
    current_timestamp() as create_dttm
  from {{ ref('t2__finanzas__egresos_detallado') }}
  where seccion = 'I'
)

select * from base
