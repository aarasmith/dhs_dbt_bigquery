{{
  config(
    materialized = "table"
  )
}}

with cs_state as (
    select
        "ISO3C" as iso3c,
        side_a_new_id as actorid,
        year,
        case
            when "Check_CS_coding" = 'yes'
            then 1
            else 0
        end cs
    from {{ source('raw', 'gov_girl_cs_2010_2020') }}
),

cs_non_state as (
    select
        "ISO3C" as iso3c,
        side_b_new_id as actorid,
        year,
        case
            when "Check_CS_coding" = 'yes'
            then 1
            else 0
        end cs
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
        {{ ref('ged_iso') }} as ged_iso
        on ged.country = ged_iso.country
    --where year > 2009
),

ged_cs_2010_2020 as (
    select
        ged.relid,
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

ged_cs as (
    select
        ged_new.relid,
        coalesce(ged_new.state_cs, 0) as state_cs,
        coalesce(ged_new.ns_cs, ged_old.cs, 0) as ns_cs
    from
        ged_cs_2010_2020 ged_new
        left join {{ ref('ns_cs_ged_1990_2011') }} ged_old
            using (relid)

)

select
    relid,
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