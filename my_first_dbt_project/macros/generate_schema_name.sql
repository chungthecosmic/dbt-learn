# schema naming 하는 macro를 override 하는 방식임!!!
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    
    {%- else -%}
        {%- if default_schema == 'prod' -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ custom_schema_name | trim }}_{{ default_schema}}}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}