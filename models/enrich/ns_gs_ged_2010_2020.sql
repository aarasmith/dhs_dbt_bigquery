

with
gs_non_state as (
    select
        "ISO3C" as iso3c,
        side_b_new_id as actorid,
        year,
        case
            when "Check_CS_coding" = 'yes'
            then 1
            else 0
        end cs,
        case
            when "Girlsoldiers" = 'yes'
            then 1
            else 0
        end gs,
        "Uncertain_girl" as uncertain_girl,
        0 as state
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
        {{ ref('ged_iso') }}
        on ged.country = ged_iso.country
    where year > 2009
)


select
    ged.relid,
    coalesce(gs_non_state.gs, 0) as ns_gs,
    coalesce(gs_non_state.cs, 0) as ns_cs
from
    ged
    inner join gs_non_state
        on gs_non_state.actorid in (ged.side_a_new_id, ged.side_b_new_id)
            and gs_non_state.year = ged.year
            and ged.iso3c = gs_non_state.iso3c

