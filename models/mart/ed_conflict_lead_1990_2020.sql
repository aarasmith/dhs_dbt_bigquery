

{% set columns = ['intensity_25km_ny', 'intensity_50km_ny',
'intensity_100km_ny'] %}


with
csl as (
    select
        *
    from {{ ref('cluster_year_stats_lead_all') }}
)

select
    {% for column in columns %}
    coalesce(csl.{{column}}, 0) as {{column}},
    {% endfor %}
    ed.*
from {{ ref('ed_conflict_1990_2020') }} ed
left join csl
    using (cid, year)