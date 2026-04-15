{% macro get_alphanumeric_count(column_name) %}

    length(regexp_replace({{ column_name }}, '[^a-zA-Z0-9]', '', 'g')) -- Count of alphanumeric characters
    
{% endmacro %}
