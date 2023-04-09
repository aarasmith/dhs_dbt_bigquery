


select
    *
from {{ ref('cluster_stats_25km') }}
left join {{ ref('cluster_stats_50km') }}
    using(cid, year)
left join {{ ref('cluster_stats_100km') }}
    using(cid, year)
