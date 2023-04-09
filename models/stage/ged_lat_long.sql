select
    id,
    year,
    latitude,
    longitude
from {{ ref('stg_ged') }}