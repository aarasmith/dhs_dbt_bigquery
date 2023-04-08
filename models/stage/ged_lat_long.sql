select
    relid,
    year,
    latitude,
    longitude
from {{ source('raw', 'ged') }}