{#
#  some specific marco functions as helper
#}

{% macro quotation_sanitize(field_name) %}
 regexp_replace( {{ field_name }} ,'"','')
{% endmacro %}
