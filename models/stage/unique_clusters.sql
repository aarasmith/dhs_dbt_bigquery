select distinct on (cid)
    cid,
    iso2c,
    long,
    lat
from {{ ref('cluster_years_stage') }}