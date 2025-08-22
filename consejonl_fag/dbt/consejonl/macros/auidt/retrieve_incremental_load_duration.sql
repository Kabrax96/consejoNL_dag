{% macro retrieve_incremental_load_duration(model_name) %}
    {% set query %}
        SELECT
            TO_CHAR(COALESCE(MAX(START_TIME), '2000-01-01'), 'YYYY-MM-DD HH24:MI:SS') AS START_TIME,
            TO_CHAR(COALESCE(MAX(END_TIME), CURRENT_TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS') AS END_TIME
        FROM CONSEJO_NL.AUDIT.DBT_JOB_LOAD_CONTROL
        WHERE MODEL_NAME = '{{ model_name }}'
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set row = results.columns[0].values() %}
        {% if row | length > 0 %}
            {% set start_time = row[0] %}
            {% set end_time = row[1] %}
            {{ return("TO_TIMESTAMP('" ~ start_time ~ "'), TO_TIMESTAMP('" ~ end_time ~ "')") }}
        {% else %}
            {{ return("TO_TIMESTAMP('2000-01-01'), CURRENT_TIMESTAMP") }}
        {% endif %}
    {% else %}
        {{ return("TO_TIMESTAMP('2000-01-01'), CURRENT_TIMESTAMP") }}
    {% endif %}
{% endmacro %}
