


with gs_state as (
    select
        "ISO3C" as iso3c,
        side_a_new_id as actorid,
        year,
        case
            when "Girlsoldiers" = 'yes'
            then 1
            else 0
        end gs,
        "Uncertain_girl" as uncertain_girl,
        1 as state
    from {{ source('raw', 'gov_girl_cs_2010_2020') }}
),

gs_non_state as (
    select
        "ISO3C" as iso3c,
        side_b_new_id as actorid,
        year,
        case
            when "Girlsoldiers" = 'yes'
            then 1
            else 0
        end gs,
        "Uncertain_girl" as uncertain_girl,
        0 as state
    from {{ source('raw', 'ns_girl_cs_2010_2020') }}
)

select
    *
from gs_state
union all 
select
    *
from gs_non_state