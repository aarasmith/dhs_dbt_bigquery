


with cs_state as (
    select
        ISO3C as iso3c,
        side_a_new_id as actorid,
        year,
        case
            when Check_CS_coding = 'yes'
            then 1
            else 0
        end cs,
        1 as state
    from {{ source('raw', 'gov_girl_cs_2010_2020') }}
),

cs_non_state as (
    select
        ISO3C as iso3c,
        side_b_new_id as actorid,
        year,
        case
            when Check_CS_coding = 'yes'
            then 1
            else 0
        end cs,
        0 as state
    from {{ source('raw', 'ns_girl_cs_2010_2020') }}
)

select
    *
from cs_state
union all 
select
    *
from cs_non_state