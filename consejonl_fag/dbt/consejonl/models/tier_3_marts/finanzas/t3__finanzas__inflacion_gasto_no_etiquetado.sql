/*
===========================================================================================================
Model Name          : t3__finanzas__inflacion_gasto_no_etiquetado
Author              : Alejandro Morales Benavides
Date                : August 12th, 2025
Usage               : dbt build --select t3__finanzas__inflacion_gasto_no_etiquetado

Objective           :
  - Compute the GNE inflation-adjusted 4-year rate for Section I (Gasto No Etiquetado),
    strictly using inflation from Dec (X-6) to Dec (X):
      (( GNE_X / ( GNE_{X-5} * PROD_{y=X-5..X}(1+infl_y) ) )^(1/4)) - 1

Dependencies        :
  - t3__finanzas__egresos_no_etiquetados  (provides MODIFICADO by date)
  - inflacion_anual (seed: year, inflation as decimal)

Assumptions/Notes   :
  - GNE = SUM(MODIFICADO) per calendar year on Section I.
  - Completeness: require 4 quarters on year X and year X-5.
  - Inflation coverage: require 6 years (X-5..X).
===========================================================================================================
*/

{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}
{{
  config(
    materialized = "table",
    snowflake_warehouse = "COMPUTE_WH",
    pre_hook = ["SET start_time = TO_TIMESTAMP('2000-01-01'); SET end_time = CURRENT_TIMESTAMP;"],
    post_hook = ["{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"]
  )
}}

-- 1) GNE by year + completeness (4 quarters)
with gne_by_year as (
  select
    date_part(year, fecha)::int                               as year,
    count(distinct cuarto)                                    as quarters_cnt,
    iff(count(distinct cuarto) = 4, true, false)              as is_complete_year,
    sum(modificado)                                           as gne
  from {{ ref('t3__finanzas__egresos_no_etiquetados') }}
  group by 1
),

years as (
  select distinct year from gne_by_year
),

infl as (
  select year, inflation
  from {{ ref('inflacion_anual') }}
),

-- 2) cumulative inflation factor for Dec (X-6) -> Dec (X)
cum_infl as (
  select
    y.year                                                                 as year_x,
    count(i.year)                                                          as infl_years_in_window,
    exp( sum( ln(1 + i.inflation) ) )                                      as cum_infl_factor
  from years y
  join infl i
    on i.year between y.year - 5 and y.year   -- covers Dec (X-6) -> Dec (X)
  group by y.year
),

-- 3) base table with diagnostics
base as (
  select
    a.year                                        as year_x,
    a.gne                                         as gne_x,
    a.is_complete_year                            as is_complete_year_x,
    a.quarters_cnt                                 as quarters_x,
    b.gne                                         as gne_x_minus_5,
    b.is_complete_year                            as is_complete_year_x_minus_5,
    b.quarters_cnt                                 as quarters_x_minus_5,
    c.cum_infl_factor                              as cum_infl_factor_x,
    c.infl_years_in_window                         as infl_years_in_window_x
  from gne_by_year a
  left join gne_by_year b on b.year = a.year - 5
  left join cum_infl      c on c.year_x = a.year
),

-- 4) final calc with strict guards
calc as (
  select
    year_x,
    gne_x,
    gne_x_minus_5,
    cum_infl_factor_x,
    quarters_x,
    quarters_x_minus_5,
    infl_years_in_window_x,
    is_complete_year_x,
    is_complete_year_x_minus_5,
    case
      when gne_x_minus_5 is not null
       and cum_infl_factor_x is not null
       and (gne_x_minus_5 * cum_infl_factor_x) > 0
       and infl_years_in_window_x = 6
       and is_complete_year_x = true
       and is_complete_year_x_minus_5 = true
      then power( gne_x / ( gne_x_minus_5 * cum_infl_factor_x ), 1.0/4 ) - 1
      else null
    end as adjusted_rate_4yr
  from base
)

select
  year_x                 as year,
  gne_x,
  gne_x_minus_5,
  cum_infl_factor_x,
  adjusted_rate_4yr,
  -- diagnostics to understand NULLs
  quarters_x,
  quarters_x_minus_5,
  infl_years_in_window_x,
  is_complete_year_x,
  is_complete_year_x_minus_5,
  current_timestamp()    as create_dttm
from calc
where year_x >= (select min(year) from gne_by_year)  -- keep all years; guards decide NULL vs value
order by year
