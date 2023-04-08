
with
cs as (
    select
        *
    from {{ ref('cluster_stats_25km') }}
    left join {{ ref('cluster_stats_50km') }}
        using(cid, year)
    left join {{ ref('cluster_stats_100km') }}
        using(cid, year)
),

cy as (
    select
        cid,
        survey_id,
        cluster,
        year
    from {{ ref('stg_cluster_years')}}
)

select
    cy.survey_id,
    cy.cluster,
    cs.*
from cy
    left join
        cs
        on cs.year = cy.year
            and cs.cid = cy.cid