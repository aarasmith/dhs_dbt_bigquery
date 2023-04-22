

select
    ged.*,
    ged_iso.iso3c,
    ged_iso.iso2c
from {{ source('raw', 'ged2021') }} ged
left join
    {{ ref('ged_iso') }} as ged_iso
    on ged.country = ged_iso.country