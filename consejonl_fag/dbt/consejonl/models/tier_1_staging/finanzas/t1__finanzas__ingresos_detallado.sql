/*
===========================================================================================================
Model Name          : t1__finanzas__ingresos_detallado
Author              : Alejandro Morales Benavides
Date                : August 12th, 2025
Usage               : dbt build --select t1__finanzas__ingresos_detallado

Objective           :
    - Create a 1:1 staging view from CONSEJO_NL.STAGING.NUEVO_LEON_INGRESOS_DETALLADO
      excluding Airbyte metadata columns, and applying minimal technical cleaning to handle NaNs and blanks.

Dependencies        :
    - Source: source('staging', 'nuevo_leon_ingresos_detallado')

Assumptions/Notes   :
    - No business transformations here.
    - Minimal cleaning performed via macros:
        * clean_date() for FECHA
        * clean_text() for text columns
        * clean_numeric() for numeric-like strings (handles symbols and parentheses)
===========================================================================================================
History             :
-----------------------------------------------------------------------------------------------------------
Name                          | Date        | Project     | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides   | 2025-08-12  | consejonl   | Created Tier 1 staging model with reusable cleaning macros.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

with src as (
    select
        -- Raw columns from staging table (excluding Airbyte metadata)
        FECHA,
        CUARTO,
        SECCION,
        CONCEPTO,
        ESTIMADO,
        DEVENGADO,
        RECAUDADO,
        DIFERENCIA,
        MODIFICADO,
        SURROGATE_KEY,
        CLAVE_PRIMARIA,
        CLAVE_SECUNDARIA,
        AMPLIACIONES_REDUCCIONES
    from {{ source('staging', 'nuevo_leon_ingresos_detallado') }}
),

normalized as (
    select
        {{ clean_date('FECHA') }}                    as FECHA,
        {{ clean_text('CUARTO') }}                   as CUARTO,
        {{ clean_text('SECCION') }}                  as SECCION,
        {{ clean_text('CONCEPTO') }}                 as CONCEPTO,
        {{ clean_numeric('ESTIMADO') }}              as ESTIMADO,
        {{ clean_numeric('DEVENGADO') }}             as DEVENGADO,
        {{ clean_numeric('RECAUDADO') }}             as RECAUDADO,
        {{ clean_numeric('DIFERENCIA') }}            as DIFERENCIA,
        {{ clean_numeric('MODIFICADO') }}            as MODIFICADO,
        {{ clean_text('SURROGATE_KEY') }}            as SURROGATE_KEY,
        {{ clean_text('CLAVE_PRIMARIA') }}           as CLAVE_PRIMARIA,
        {{ clean_text('CLAVE_SECUNDARIA') }}         as CLAVE_SECUNDARIA,
        {{ clean_numeric('AMPLIACIONES_REDUCCIONES') }} as AMPLIACIONES_REDUCCIONES
    from src
)

select *
from normalized
