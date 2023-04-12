{{
  config(
    materialized = "table",
    indexes=[
        {'columns': ['cid', 'year', 'dist'], 'unique': True}
    ]
  )
}}

{% set columns = get_conflict_vars() %}

select
    cid,
    year,
    dist,
    count(id) as n_events,
    {% for column in columns %}
    sum({{column}}) as {{column}}
    {{ "," if not loop.last }}
    {% endfor %}
from {{ ref('ged_joined') }}
where id is not null
group by
    cid,
    year,
    dist
