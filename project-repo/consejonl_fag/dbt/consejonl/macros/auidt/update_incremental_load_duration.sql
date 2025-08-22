{% macro update_incremental_load_duration(model_name, start_time) %}
    {% set sql %}
        MERGE INTO CONSEJO_NL.AUDIT.DBT_JOB_LOAD_CONTROL AS target
        USING (
            SELECT
                '{{ model_name }}' AS MODEL_NAME,
                TO_TIMESTAMP('{{ start_time }}') AS START_TIME,
                CURRENT_TIMESTAMP AS END_TIME
        ) AS source
        ON target.MODEL_NAME = source.MODEL_NAME
        WHEN MATCHED THEN
            UPDATE SET
                target.START_TIME = source.START_TIME,
                target.END_TIME = source.END_TIME,
                target.LAST_UPDATED = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (MODEL_NAME, START_TIME, END_TIME, LAST_UPDATED)
            VALUES (source.MODEL_NAME, source.START_TIME, source.END_TIME, CURRENT_TIMESTAMP);
    {% endset %}

    {% do run_query(sql) %}
{% endmacro %}
