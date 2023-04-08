select
    *
from {{ ref('cluster_stats_lead_25km') }}
left join {{ ref('cluster_stats_lead_50km') }}
    using (cid, year)
left join {{ ref('cluster_stats_lead_100km') }}
    using (cid, year)