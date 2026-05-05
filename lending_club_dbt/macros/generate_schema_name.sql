{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
      Override default behaviour so seeds/models with +schema config
      use that exact schema name instead of <target_schema>_<custom>.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
