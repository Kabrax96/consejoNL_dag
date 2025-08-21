/*
===========================================================================================================
Model Name          : t2__finanzas__ingresos_detallado
Author              : Alejandro Morales Benavides
Date                : August 12th, 2025
Usage               : dbt build --select t2__finanzas__ingresos_detallado

Objective           :
    This intermediate model:
      1) Consumes Tier 1 cleaned staging for Ingresos Detallado.
      2) Preserves business fields without redefining semantics.
      3) Adds audit metadata (CREATE_DTTM) and prepares the dataset for Tier 3 marts.
      4) Implements incremental loading using FECHA as the watermark.

Dependencies        :
    - Source model: t1__finanzas__ingresos_detallado

Assumptions/Notes   :
    - FECHA is a DATE (typed in Tier 1 via macros); used as the incremental filter.
    - Column names match Tier 1: FECHA, CUARTO, SECCION, CONCEPTO, ESTIMADO, DEVENGADO, RECAUDADO,
      DIFERENCIA, MODIFICADO, SURROGATE_KEY, CLAVE_PRIMARIA, CLAVE_SECUNDARIA, AMPLIACIONES_REDUCCIONES.
    - No business transformations are applied here (only light audit/structural setup).
    - unique_key = SURROGATE_KEY; on_schema_change = append_new_columns.

===========================================================================================================
History             :
-----------------------------------------------------------------------------------------------------------
Name                          | Date        | Project     | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides   | 2025-08-12  | consejonl   | Created Tier 2 model for Ingresos Detallado (incremental + audit).
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/


{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}

{{
    config(
        materialized = "incremental",
        unique_key = ["surrogate_key"],
        on_schema_change = "append_new_columns",
        merge_exclude_columns = ["CREATE_DTTM"],
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
        cuarto,
        seccion,
        concepto,
        estimado,
        devengado,
        recaudado,
        diferencia,
        modificado,
        surrogate_key,
        clave_primaria,
        clave_secundaria,
        ampliaciones_reducciones,
        current_timestamp() as create_dttm
    from {{ ref('t1__finanzas__ingresos_detallado') }}
    {% if is_incremental() %}
      where fecha > (select coalesce(max(fecha), '2000-01-01') from {{ this }})
    {% endif %}
)

select * from base
