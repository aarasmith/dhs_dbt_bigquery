
{% set columns = get_conflict_vars() %}

{% set columns_ny = get_lead_vars() %}

with
cluster_years as(
    select
        cid,
        year,
        survey_id,
        region,
        cluster
    from {{ ref('stg_cluster_years') }}
)


select
    cy.survey_id,
    cy.region,
    cy.cluster,
    cy.year,
    cid,
    {% for column in columns %}
    coalesce(cs.{{column}}_25km, 0) as {{column}}_25km,
    coalesce(cs.{{column}}_50km, 0) as {{column}}_50km,
    coalesce(cs.{{column}}_100km, 0) as {{column}}_100km,
    {% endfor %}
    {% for column in columns_ny %}
    coalesce(csl.{{column}}_25km_ny, 0) as {{column}}_25km_ny,
    coalesce(csl.{{column}}_50km_ny, 0) as {{column}}_50km_ny,
    coalesce(csl.{{column}}_100km_ny, 0) as {{column}}_100km_ny
    {{ "," if not loop.last }}
    {% endfor %}
from cluster_years cy
left join {{ ref('cluster_year_stats_all') }} cs
    using(cid, year)
left join {{ ref('cluster_year_stats_lead_all') }} csl
    using(cid, year)