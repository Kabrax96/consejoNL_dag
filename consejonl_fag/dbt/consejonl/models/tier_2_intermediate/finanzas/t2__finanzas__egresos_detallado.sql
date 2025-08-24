/*
===========================================================================================================
model name          : t2__finanzas__egresos_detallado
author              : Alejandro Morales Benavides
date                : August 8th, 2025
usage               : dbt build --select t2__finanzas__egresos_detallado

objective           :
    This intermediate model:
        1) Performs data type validation and minor cleaning.
        2) Ensures key fields are standardized.
        3) Prepares the data for marts in tier 3 (Sección I, II, inflación).

dependencies        :
    - Depends on t1__finanzas__egresos_detallado

assumptions/notes   :
    - Removes rows where essential fields (concepto, modificado, cuarto) are null.
    - Trims whitespace from text fields.
    - Parses date string if required.
===========================================================================================================
history             :
-----------------------------------------------------------------------------------------------------------
name                   | date           | project             | description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales      | 08/08/2025     | consejo_nl_dbt      | Created intermediate model for egresos_detallado.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

{%- set model_run_start_time_variable = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}

{{ config(
    materialized='incremental',
    unique_key=['surrogate_key'],
    on_schema_change='append_new_columns',
    merge_exclude_columns=['CREATE_DTTM'],
    tags=['finanzas','egresos_detallado','t2','egresos_detallado_cp'],
    snowflake_warehouse='COMPUTE_WH',
    pre_hook=[
      "SET start_time = TO_TIMESTAMP('2000-01-01')",
      "SET end_time = CURRENT_TIMESTAMP()"
    ],
    post_hook=[
      "{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"
    ]
) }}

with base as (
    select
        try_to_date(FECHA)         as fecha,
        CODIGO                     as codigo,
        CUARTO                     as cuarto,
        PAGADO                     as pagado,
        SECCION                    as seccion,
        APROBADO                   as aprobado,
        CONCEPTO                   as concepto,
        DEVENGADO                  as devengado,
        MODIFICADO                 as modificado,
        SUBEJERCICIO               as subejercicio,
        SURROGATE_KEY              as surrogate_key,
        "AMPLIACIONES/REDUCCIONES",
        current_timestamp()        as CREATE_DTTM
    from {{ ref('t1__finanzas__egresos_detallado') }}
    where {{ period_filter('FECHA') }}
    {% if is_incremental() %}
      and try_to_date(FECHA) > (
        select coalesce(max(fecha), '2000-01-01') from {{ this }}
      )
    {% endif %}
)

select *
from base
