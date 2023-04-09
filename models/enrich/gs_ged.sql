

with gs_state as (
    select
        iso3c,
        actorid,
        year,
        gs
    from {{ ref('stg_gs_2010_2020') }}
    where state = 1
),

gs_non_state as (
    select
        iso3c,
        actorid,
        year,
        gs
    from {{ ref('stg_gs_2010_2020') }}
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
    from {{ ref('stg_ged') }}
),

ged_gs as (
    select
        ged.id,
        coalesce(gs_state.gs, 0) as state_gs,
        coalesce(gs_non_state.gs, 0) as ns_gs
    from
        ged
        left join gs_state
            on gs_state.actorid in (ged.side_a_new_id, ged.side_b_new_id)
                and gs_state.year = ged.year
                and ged.iso3c = gs_state.iso3c
        left join gs_non_state
            on gs_non_state.actorid in (ged.side_a_new_id, ged.side_b_new_id)
                and gs_non_state.year = ged.year
                and ged.iso3c = gs_non_state.iso3c

)

select
    id,
    state_gs,
    ns_gs,
    --both
    case
        when state_gs = 1 and ns_gs = 1
        then 1
        else 0
    end both_gs,
    --only_state
    case
        when state_gs = 1 and ns_gs = 0
        then 1
        else 0
    end only_state_gs,
    --only_non_state
    case
        when state_gs = 0 and ns_gs = 1
        then 1
        else 0
    end only_ns_gs,
    --any
    case
        when state_gs = 1 or ns_gs = 1
        then 1
        else 0
    end gs
from ged_gs