

select
    ged.*,
    ged_iso.iso3c
from {{ source('raw', 'ged') }}
left join
    {{ ref('ged_iso') }} as ged_iso
    on ged.country = ged_iso.country