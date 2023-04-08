{{
  config(
    materialized = "table"
  )
}}

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
        "Uncertain_girl" as uncertain_girl
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
        "Uncertain_girl" as uncertain_girl
    from {{ source('raw', 'ns_girl_cs_2010_2020') }}
),

ged as (
    select
        relid,
        year,
        side_a_new_id,
        side_b_new_id,
        conflict_new_id,
        ged_iso.iso3c
    from {{ source('raw', 'ged') }} ged
    left join
        {{ ref('ged_iso') }} ged_iso
        on ged.country = ged_iso.country
    --where year > 2009
),

ged_gs as (
    select
        ged.relid,
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
    relid,
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