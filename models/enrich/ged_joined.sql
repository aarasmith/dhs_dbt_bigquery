{{
  config(
    materialized = "table"
  )
}}

with
ged_cluster as (
    select
        relid,
        year,
        cid,
        dist
    from {{ ref('ged_cluster') }}
),

ged_joined as (
    select
        *
    from
    ged_cluster
    left join
        {{ ref('sv_ged') }} sv_ged
        using (relid)
    left join
        {{ ref('gs_ged') }} gs_ged
        using (relid)
    left join
        {{ ref('ged_conflict_type') }} ged_conflict_type
        using (relid)
    left join
        {{ ref('cs_ged') }}
        using (relid)
)

select * from ged_joined