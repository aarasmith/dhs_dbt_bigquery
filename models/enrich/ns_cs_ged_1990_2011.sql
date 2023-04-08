

select
    ged.id,
    coalesce(cs.cs, 0) as cs
from
    {{ source('raw', 'ged') }} ged
left join
    {{ ref('stg_cs_1990_2011') }} cs
on cs.sideb in (ged.side_a, ged.side_b)
    and cs.year = ged.year
    and cs.location = ged.country
--where ged.year < 2010