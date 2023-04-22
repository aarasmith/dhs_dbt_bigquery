


select
    cid,
    year,
    cumulative_conflict_years_25km,
    cumulative_conflict_years_50km,
    cumulative_conflict_years_100km
from {{ ref('cluster_cumulative_conflict_25km') }}
left join {{ ref('cluster_cumulative_conflict_50km') }}
    using(cid, year)
left join {{ ref('cluster_cumulative_conflict_100km') }}
    using(cid, year)