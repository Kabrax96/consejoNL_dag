/*
===========================================================================================================
Model Name          : t3__finanzas__balance_presupuestario_indicator
Author              : Alejandro Morales Benavides
Date                : August 6th, 2025
Usage               : dbt build --select t3__finanzas__balance_presupuestario_indicator
Objective           :
    This model:
        1) Aggregates and calculates the final indicator for the Balance Presupuestario.
        2) Represents the final business-ready metric used in dashboards or reporting.

Dependencies        :
    - Source model: t2__finanzas__balance_presupuestario

Assumptions/Notes   :
    - YEAR_QUARTER is the temporal grain for this indicator.
    - AMOUNT contains the total net value for the indicator.

===========================================================================================================
History             :
-----------------------------------------------------------------------------------------------------------
Name                             | Date           | Project             | Description
-----------------------------------------------------------------------------------------------------------
Alejandro Morales Benavides      | 2025-08-06     | consejo_nl          | Created tier 3 model for Balance Presupuestario.
-----------------------------------------------------------------------------------------------------------
===========================================================================================================
*/

{%- set model_run_start_time_variable = modules.datetime.datetime.now().astimezone(modules.pytz.timezone("America/Mexico_City")) -%}

{{
    config(
        materialized = "table",
        unique_key = ["YEAR_QUARTER"],
        on_schema_change = "append_new_columns",
        merge_exclude_columns = ["CREATE_TMS", "CREATE_BY"],
        tags = ["program:consejo_nl", "indicator:balance_presupuestario", "layer:t3"],
        snowflake_warehouse = "COMPUTE_WH",
        pre_hook = ["SET start_time = TO_TIMESTAMP('2000-01-01'); SET end_time = CURRENT_TIMESTAMP;"
        ],
        post_hook = [
            "{{ update_incremental_load_duration('" ~ this.identifier ~ "', '" ~ model_run_start_time_variable ~ "') }}"
        ]
    )
}}

with source_data as (
    select *
    from {{ ref('t2__finanzas__balance_presupuestario') }}
),

final as (
    select
        YEAR_QUARTER,
        sum(AMOUNT) as total_balance_presupuestario,
        max(FULL_DATE) as latest_date,
        current_timestamp as CREATE_TMS,
        'dbt_user' as CREATE_BY
    from source_data
    group by YEAR_QUARTER
)

select * from final
