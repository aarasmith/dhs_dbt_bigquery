{{
  config(
    materialized = "table",
    indexes=[
        {'columns': ['id']}
    ]
  )
}}

with 
ged as (
    select
        id,
        type_of_violence as tov,
        best
    from {{ source('raw', 'ged') }}
)

select
    id,
    --tov as type_of_violence,
    best,
    case
        when tov = 1
        then best
        else 0
    end state_best,
    case
        when tov = 2
        then best
        else 0
    end ns_best,
    --one-sided violence
    case
        when tov = 3
        then best
        else 0
    end os_best
from ged