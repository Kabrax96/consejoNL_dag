/*
===========================================================================================================
model name          : t1__finanzas__balance_presupuestario
author              : Alejandro Morales Benavides
date                : August 6th, 2025
usage               : dbt build --select t1__finanzas__balance_presupuestario

objective           :
    this model performs the following:
        1) creates a snowflake staging view for 1-1 mapping from source table (consejo_nl.staging.nuevo_leon_balance_presupuestario).

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
Alejandro Morales      | 08/06/2025     | consejo_nl_dbt      | created raw tier 1 model from snowflake source.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

with source as (
    select
        TYPE,
        AMOUNT,
        CONCEPT,
        SUBLABEL,
        FULL_DATE,
        YEAR_QUARTER,
        SURROGATE_KEY
    from {{ source('staging', 'nuevo_leon_balance_presupuestario') }}
)

select * from source
