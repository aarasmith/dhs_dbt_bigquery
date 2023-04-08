select distinct on (cid)
    cid,
    iso2c,
    long,
    lat
from {{ ref('stg_cluster_years') }}