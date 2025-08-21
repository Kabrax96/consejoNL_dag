/*
===========================================================================================================
model name          : t1__finanzas__egresos_detallado
author              : Alejandro Morales Benavides
date                : August 8th, 2025
usage               : dbt build --select t1__finanzas__egresos_detallado

objective           :
    this model performs the following:
        1) creates a snowflake staging view for 1-1 mapping from source table (consejo_nl.staging.nuevo_leon_egresos_detallado).

dependencies        :
    1) source table is present and is getting refreshed.

assumptions/notes   :
    - airbyte columns are excluded.
    - source table is not altered.
===========================================================================================================
history             :
-----------------------------------------------------------------------------------------------------------
name                   | date           | project             | description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales      | 08/08/2025     | consejo_nl_dbt      | created raw tier 1 model from snowflake source.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

with source as (
    select
        FECHA,
        CODIGO,
        CUARTO,
        PAGADO,
        SECCION,
        APROBADO,
        CONCEPTO,
        DEVENGADO,
        MODIFICADO,
        SUBEJERCICIO,
        SURROGATE_KEY,
        "AMPLIACIONES/REDUCCIONES"
    from {{ source('staging', 'nuevo_leon_egresos_detallado') }}
)

select * from source
