/*
===========================================================================================================
Model Name          : t3__finanzas__ip_sobre_ild
Author              : Alejandro Morales Benavides
Date                : August 12th, 2025
Usage               : dbt build --select t3__finanzas__ip_sobre_ild

Objective           :
    This model:
      1) Computes Propios Income (IP) = impuestos + derechos + productos + aprovechamientos.
      2) Computes Ingresos de Libre Disposición (ILD).
      3) Outputs the ratio IP / ILD * 100 at two grains: by calendar year and by quarter.

Dependencies        :
    - Source model: t2__finanzas__ingresos_detallado

Assumptions/Notes   :
    - Concepts matched case-insensitively, accents removed.
    - ILD is identified via SECCION = 'INGRESOS DE LIBRE DISPOSICION' (adjust if your exact label differs).
    - If your labels vary, edit the concept list at the top (propios_conceptos) or the ILD label constant.

===========================================================================================================
History             :
-----------------------------------------------------------------------------------------------------------
Name                          | Date        | Project     | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides   | 2025-08-12  | consejonl   | Created Tier 3 model for IP / ILD * 100 indicator.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}

{# =========================
   Parameters / Constants
   ========================= #}
{# Lista de conceptos que suman IP (sin acentos, en mayúsculas) #}
{%- set propios_conceptos = ['IMPUESTOS','DERECHOS','PRODUCTOS','APROVECHAMIENTOS'] -%}

{# Etiqueta exacta (sin acentos, en mayúsculas) para ILD en SECCION #}
{%- set ild_label = 'INGRESOS DE LIBRE DISPOSICION' -%}

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

with t2 as (
    select
        fecha,
        cuarto,
        seccion,
        concepto,
        recaudado,
        modificado
    from {{ ref('t2__finanzas__ingresos_detallado') }}
),

-- Normaliza textos: mayúsculas y sin acentos
norm as (
    select
        fecha,
        cuarto,
        upper(translate(seccion, 'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ', 'AEIOUAEIOUAEIOUAEIOUNN')) as seccion_norm,
        upper(translate(concepto,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ', 'AEIOUAEIOUAEIOUAEIOUNN')) as concepto_norm,
        recaudado,
        modificado
    from t2
),

-- Agregación por AÑO
by_year as (
    select
        date_part(year, fecha)::int as year,
        sum(case when concepto_norm in ({% for c in propios_conceptos %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then coalesce(recaudado,0) else 0 end) as ip_recaudado,
        sum(case when seccion_norm = '{{ ild_label }}' then coalesce(recaudado,0) else 0 end) as ild_recaudado
    from norm
    group by 1
),

-- Agregación por CUARTO
by_quarter as (
    select
        cuarto,
        date_part(year, min(fecha))::int as year,  -- referencia del año
        sum(case when concepto_norm in ({% for c in propios_conceptos %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then coalesce(recaudado,0) else 0 end) as ip_recaudado,
        sum(case when seccion_norm = '{{ ild_label }}' then coalesce(recaudado,0) else 0 end) as ild_recaudado
    from norm
    group by 1
),

calc_year as (
    select
        year,
        ip_recaudado,
        ild_recaudado,
        case when ild_recaudado > 0 then (ip_recaudado / ild_recaudado) * 100 else null end as ip_sobre_ild_pct
    from by_year
),

calc_quarter as (
    select
        year,
        cuarto,
        ip_recaudado,
        ild_recaudado,
        case when ild_recaudado > 0 then (ip_recaudado / ild_recaudado) * 100 else null end as ip_sobre_ild_pct
    from by_quarter
)

-- Resultado final: dos vistas unificadas con el mismo set de columnas
select
    'YEAR' as grain,
    cast(null as varchar) as cuarto,
    year,
    ip_recaudado,
    ild_recaudado,
    ip_sobre_ild_pct,
    current_timestamp() as create_dttm
from calc_year

union all

select
    'QUARTER' as grain,
    cuarto,
    year,
    ip_recaudado,
    ild_recaudado,
    ip_sobre_ild_pct,
    current_timestamp() as create_dttm
from calc_quarter
