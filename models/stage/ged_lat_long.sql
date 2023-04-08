select
    id,
    year,
    latitude,
    longitude
from {{ source('raw', 'ged') }}