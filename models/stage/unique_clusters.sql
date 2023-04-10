select
    cid,
    iso2c,
    long,
    lat
from {{ ref('stg_cluster_years') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY cid) = 1