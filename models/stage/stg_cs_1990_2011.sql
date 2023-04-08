


select
    sideb,
    year,
    location,
    "Csdum" as cs
from {{ source('raw', 'ns_roos_1990_2011')}}
where "Csdum" = 1