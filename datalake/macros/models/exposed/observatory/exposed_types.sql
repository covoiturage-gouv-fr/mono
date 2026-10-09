{#
  --vars '{exposed_types: [custom]}' : l'exposé ne recalcule que ces types, sur tout leur
  historique, après suppression de leurs lignes (pre_hook). Les autres types ne sont ni lus
  ni touchés. Sans la variable : incrémental habituel (dernière période).
#}

{% macro exposed_types() %}
  {%- set types = var('exposed_types', none) -%}
  {{ return([types] if types is string else types) }}
{% endmacro %}

{% macro exposed_incremental() %}
  {{ return(is_incremental() and not exposed_types()) }}
{% endmacro %}

{% macro exposed_type_map(type_map) %}
  {%- set types = exposed_types() -%}
  {%- if not types -%}
    {{ return(type_map) }}
  {%- endif -%}
  {#- Sans table existante (premier build, --full-refresh), on ne bâtirait que ces types. -#}
  {%- if execute and not is_incremental() -%}
    {{ exceptions.raise_compiler_error("exposed_types exige une table existante, sans --full-refresh : " ~ this) }}
  {%- endif -%}
  {%- set kept = [] -%}
  {%- for t in type_map -%}
    {%- if t[1] in types -%}{%- do kept.append(t) -%}{%- endif -%}
  {%- endfor -%}
  {%- if not kept -%}
    {{ exceptions.raise_compiler_error("exposed_types " ~ types ~ " : aucun type exposé par " ~ this) }}
  {%- endif -%}
  {{ return(kept) }}
{% endmacro %}

{% macro exposed_types_delete() %}
  DELETE FROM {{ this }} WHERE type IN (
    {%- for t in exposed_types() %}'{{ t }}'{% if not loop.last %}, {% endif %}{% endfor -%}
  )
{% endmacro %}
