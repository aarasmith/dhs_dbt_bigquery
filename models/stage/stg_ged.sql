select
    *,
    row_number() over (order by relid) as relid_new
from {{ source('raw', 'ged') }} 