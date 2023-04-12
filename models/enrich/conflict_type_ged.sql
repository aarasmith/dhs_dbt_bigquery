

with 
ged as (
    select
        id,
        type_of_violence as tov,
        best
    from {{ ref('stg_ged') }}
)

select
    id,
    --tov as type_of_violence,
    best as intensity,
    case
        when tov = 1
        then best
        else 0
    end state_intensity,
    case
        when tov = 2
        then best
        else 0
    end ns_intensity,
    --one-sided violence
    case
        when tov = 3
        then best
        else 0
    end os_intensity
from ged