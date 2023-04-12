select
    id,
    year,
    iso2c as ged_iso2c,
    latitude,
    longitude
from {{ ref('stg_ged') }}