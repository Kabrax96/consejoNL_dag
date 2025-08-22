{% macro get_warehouse_based_on_load_type() %}
    {% set environment = env_var('DBT_ENVIRONMENT', 'dev') %}

    {% if environment == 'prod' %}
        WH_PROD
    {% elif environment == 'qa' %}
        WH_QA
    {% else %}
        WH_DEV
    {% endif %}
{% endmacro %}
