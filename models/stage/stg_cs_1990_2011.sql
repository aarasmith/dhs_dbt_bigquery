


select
    sideb,
    year,
    location,
    cast ("Csdum" as INTEGER) as cs
from {{ source('raw', 'ns_roos_1990_2011')}}
where "Csdum" = 1