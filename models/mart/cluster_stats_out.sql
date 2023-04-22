

with
cc as (
    select
        cid,
        year,
        cumulative_conflict_years_25km,
        cumulative_conflict_years_50km,
        cumulative_conflict_years_100km
    from {{ ref('cluster_cumulative_conflict_all') }}
)

select
    *
from {{ ref('cluster_year_stats_all_with_lead') }} cs
left join cc
    using(cid, year)