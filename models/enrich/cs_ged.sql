

with cs_state as (
    select
        iso3c,
        actorid,
        year,
        cs
    from {{ ref('stg_cs_2010_2020') }}
    where state = 1
),

cs_non_state as (
    select
        iso3c,
        actorid,
        year,
        cs
    from {{ ref('stg_cs_2010_2020') }}
    where state = 0
),

ged as (
    select
        id,
        year,
        side_a_new_id,
        side_b_new_id,
        conflict_new_id,
        iso3c
    from {{ ref('stg_ged') }} ged
),

ged_cs_2010_2020 as (
    select
        ged.id,
        coalesce(cs_state.cs, 0) as state_cs,
        coalesce(cs_non_state.cs, 0) as ns_cs
    from
        ged
        left join cs_state
            on cs_state.actorid in (ged.side_a_new_id, ged.side_b_new_id)
                and cs_state.year = ged.year
                and ged.iso3c = cs_state.iso3c
        left join cs_non_state
            on cs_non_state.actorid in (ged.side_a_new_id, ged.side_b_new_id)
                and cs_non_state.year = ged.year
                and ged.iso3c = cs_non_state.iso3c

),

ged_cs_1990_2011 as (
    select
        ged.id,
        coalesce(cs.cs, 0) as cs
    from
        {{ ref('stg_ged') }} ged
    left join
        {{ ref('stg_cs_1990_2011') }} cs
    on cs.sideb in (ged.side_a, ged.side_b)
        and cs.year = ged.year
        and cs.location = ged.country
),

ged_cs as (
    select
        ged_new.id,
        coalesce(ged_new.state_cs, 0) as state_cs,
        coalesce(ged_new.ns_cs, ged_old.cs, 0) as ns_cs
    from
        ged_cs_2010_2020 ged_new
        left join ged_cs_1990_2011 ged_old
            using (id)

)

select
    id,
    state_cs,
    ns_cs,
    --both
    case
        when state_cs = 1 and ns_cs = 1
        then 1
        else 0
    end both_cs,
    --only_state
    case
        when state_cs = 1 and ns_cs = 0
        then 1
        else 0
    end only_state_cs,
    --only_non_state
    case
        when state_cs = 0 and ns_cs = 1
        then 1
        else 0
    end only_ns_cs,
    --any
    case
        when state_cs = 1 or ns_cs = 1
        then 1
        else 0
    end cs
from ged_cs