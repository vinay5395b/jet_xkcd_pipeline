{% macro get_comic_cost(column_name) %}
    -- removing anything that is NOT a letter (a-z, A-Z)
    -- then we count the length and multiply by 5
    (length(regexp_replace({{ column_name }}, '[^a-zA-Z]', '', 'g')) * 5)
{% endmacro %}
