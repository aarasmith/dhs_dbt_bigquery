{{
  config(
    materialized = "table",
    indexes=[
        {'columns': ['id', 'year', 'dist']}
    ]
  )
}}

with
ged_cluster as (
    select
        cast (id as INTEGER) as id,
        cast (year as INTEGER) as year,
        cid,
        cast (dist as INTEGER) as dist
    from {{ ref('ged_cluster') }}
),

ged_joined as (
    select
        *
    from
    ged_cluster
    left join
        {{ ref('sv_ged') }} sv_ged
        using (id)
    left join
        {{ ref('gs_ged') }} gs_ged
        using (id)
    left join
        {{ ref('ged_conflict_type') }} ged_conflict_type
        using (id)
    left join
        {{ ref('cs_ged') }}
        using (id)
)

select * from ged_joined