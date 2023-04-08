{{
  config(
    materialized = "table"
  )
}}

with 
ged as (
    select
        relid,
        type_of_violence as tov,
        best
    from {{ source('raw', 'ged') }}
)

select
    relid,
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