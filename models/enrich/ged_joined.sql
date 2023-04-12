

with
cluster_ged as (
    select
        cast (id as INTEGER) as id,
        cast (year as INTEGER) as year,
        cid,
        cast (dist as INTEGER) as dist
    from {{ ref('cluster_ged') }}
    where cluster_iso2c = ged_iso2c
),

ged_joined as (
    select
        *
    from
    cluster_ged
    left join
        {{ ref('sv_ged') }} sv_ged
        using (id)
    left join
        {{ ref('gs_ged') }} gs_ged
        using (id)
    left join
        {{ ref('conflict_type_ged') }} ged_conflict_type
        using (id)
    left join
        {{ ref('cs_ged') }}
        using (id)
)

select * from ged_joined