{{
  config(
    materialized = "table"
  )
}}

with sv_state as (
    select * from {{ ref('sv_state') }}
),

sv_non_state as (
    select * from {{ ref('sv_non_state') }}
),

ged as (
    select
        relid,
        year,
        side_a_new_id,
        side_b_new_id,
        conflict_new_id,
        country_id
    from {{ source('raw', 'ged') }}
),

ged_sv as (
    select
        ged.relid,
        coalesce(sv_state.adult_prev_minor, 0) as state_adult_prev_minor,
        coalesce(sv_state.adult_prev_major, 0) as state_adult_prev_major,
        coalesce(sv_state.child_prev, 0) as state_child_prev,
        coalesce(sv_non_state.adult_prev_minor, 0) as ns_adult_prev_minor,
        coalesce(sv_non_state.adult_prev_major, 0) as ns_adult_prev_major,
        coalesce(sv_non_state.child_prev, 0) as ns_child_prev
    from
        ged
        left join sv_state
            on sv_state.actorid in (ged.side_a_new_id, ged.side_b_new_id)
                and sv_state.year = ged.year
                and sv_state.conflictid = ged.conflict_new_id
                and ged.country_id in (sv_state.gwnoloc, sv_state.gwnoloc2)
        left join sv_non_state
            on sv_non_state.actorid in (ged.side_a_new_id, ged.side_b_new_id)
                and sv_non_state.year = ged.year
                and sv_non_state.conflictid = ged.conflict_new_id
                and ged.country_id in (sv_non_state.gwnoloc, sv_non_state.gwnoloc2)

)

select
    relid,
    state_adult_prev_minor,
    state_adult_prev_major,
    state_child_prev,
    ns_adult_prev_minor,
    ns_adult_prev_major,
    ns_child_prev,
    --both
    case
        when state_adult_prev_major = 1 and ns_adult_prev_major = 1
        then 1
        else 0
    end both_adult_prev_major,
    case
        when state_adult_prev_minor = 1 and ns_adult_prev_minor = 1
        then 1
        else 0
    end both_adult_prev_minor,
    case
        when state_child_prev = 1 and ns_child_prev = 1
        then 1
        else 0
    end both_child_prev,
    --only_state
    case
        when state_adult_prev_major = 1 and ns_adult_prev_major = 0
        then 1
        else 0
    end only_state_adult_prev_major,
    case
        when state_adult_prev_minor = 1 and ns_adult_prev_minor = 0
        then 1
        else 0
    end only_state_adult_prev_minor,
    case
        when state_child_prev= 1 and ns_child_prev = 0
        then 1
        else 0
    end only_state_child_prev,
    --only_non_state
    case
        when state_adult_prev_major = 0 and ns_adult_prev_major = 1
        then 1
        else 0
    end only_ns_adult_prev_major,
    case
        when state_adult_prev_minor = 0 and ns_adult_prev_minor = 1
        then 1
        else 0
    end only_ns_adult_prev_minor,
    case
        when state_child_prev= 0 and ns_child_prev = 1
        then 1
        else 0
    end only_ns_child_prev,
    --any
    case
        when state_adult_prev_major = 1 or ns_adult_prev_major = 1
        then 1
        else 0
    end adult_prev_major,
    case
        when state_adult_prev_minor = 1 or ns_adult_prev_minor = 1
        then 1
        else 0
    end adult_prev_minor,
    case
        when state_child_prev= 1 or ns_child_prev = 1
        then 1
        else 0
    end child_prev
from ged_sv