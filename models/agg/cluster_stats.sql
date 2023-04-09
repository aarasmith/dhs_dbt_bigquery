{{
  config(
    materialized = "table",
    indexes=[
        {'columns': ['cid', 'year', 'dist'], 'unique': True}
    ],
    pre_hook = ["set local work_mem = '128MB'"],
    post_hook = ["reset work_mem"]
  )
}}

{% set columns = ['state_adult_prev_minor',
       'state_adult_prev_major', 'state_child_prev', 'ns_adult_prev_minor',
       'ns_adult_prev_major', 'ns_child_prev', 'both_adult_prev_major',
       'both_adult_prev_minor', 'both_child_prev',
       'only_state_adult_prev_major', 'only_state_adult_prev_minor',
       'only_state_child_prev', 'only_ns_adult_prev_major',
       'only_ns_adult_prev_minor', 'only_ns_child_prev', 'adult_prev_major',
       'adult_prev_minor', 'child_prev', 'state_gs', 'ns_gs', 'both_gs',
       'only_state_gs', 'only_ns_gs', 'gs', 'state_cs', 'ns_cs', 'both_cs',
       'only_state_cs', 'only_ns_cs', 'cs'] %}

select
    cid,
    year,
    dist,
    count(id) as n_events,
    {% for column in columns %}
    sum({{column}}) as {{column}},
    {% endfor %}
    sum(best) as intensity,
    sum(os_best) as os_intensity,
    sum(state_best) as state_intensity,
    sum(ns_best) as ns_intensity
from {{ ref('ged_joined') }}
where id is not null
group by
    cid,
    year,
    dist
