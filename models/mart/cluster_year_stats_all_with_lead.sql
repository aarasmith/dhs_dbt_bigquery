

{% set columns = ['state_adult_prev_minor_25km', 'state_adult_prev_major_25km',
'state_child_prev_25km', 'ns_adult_prev_minor_25km',
'ns_adult_prev_major_25km','ns_child_prev_25km',
'both_adult_prev_major_25km', 'both_adult_prev_minor_25km',
'both_child_prev_25km', 'only_state_adult_prev_major_25km',
'only_state_adult_prev_minor_25km', 'only_state_child_prev_25km',
'only_ns_adult_prev_major_25km', 'only_ns_adult_prev_minor_25km',
'only_ns_child_prev_25km', 'adult_prev_major_25km', 'adult_prev_minor_25km',
'child_prev_25km', 'state_gs_25km', 'ns_gs_25km', 'both_gs_25km',
'only_state_gs_25km', 'only_ns_gs_25km', 'gs_25km', 'state_cs_25km',
'ns_cs_25km', 'both_cs_25km', 'only_state_cs_25km', 'only_ns_cs_25km',
'cs_25km', 'intensity_25km', 'os_intensity_25km', 'state_intensity_25km',
'ns_intensity_25km', 'state_adult_prev_minor_50km',
'state_adult_prev_major_50km', 'state_child_prev_50km',
'ns_adult_prev_minor_50km', 'ns_adult_prev_major_50km',
'ns_child_prev_50km', 'both_adult_prev_major_50km',
'both_adult_prev_minor_50km', 'both_child_prev_50km',
'only_state_adult_prev_major_50km', 'only_state_adult_prev_minor_50km',
'only_state_child_prev_50km', 'only_ns_adult_prev_major_50km',
'only_ns_adult_prev_minor_50km', 'only_ns_child_prev_50km',
'adult_prev_major_50km', 'adult_prev_minor_50km', 'child_prev_50km',
'state_gs_50km', 'ns_gs_50km', 'both_gs_50km', 'only_state_gs_50km',
'only_ns_gs_50km', 'gs_50km', 'state_cs_50km', 'ns_cs_50km', 'both_cs_50km',
'only_state_cs_50km', 'only_ns_cs_50km', 'cs_50km', 'intensity_50km',
'os_intensity_50km', 'state_intensity_50km', 'ns_intensity_50km',
'state_adult_prev_minor_100km', 'state_adult_prev_major_100km',
'state_child_prev_100km', 'ns_adult_prev_minor_100km',
'ns_adult_prev_major_100km', 'ns_child_prev_100km',
'both_adult_prev_major_100km', 'both_adult_prev_minor_100km',
'both_child_prev_100km', 'only_state_adult_prev_major_100km',
'only_state_adult_prev_minor_100km', 'only_state_child_prev_100km',
'only_ns_adult_prev_major_100km', 'only_ns_adult_prev_minor_100km',
'only_ns_child_prev_100km', 'adult_prev_major_100km',
'adult_prev_minor_100km', 'child_prev_100km', 'state_gs_100km',
'ns_gs_100km', 'both_gs_100km', 'only_state_gs_100km', 'only_ns_gs_100km',
'gs_100km', 'state_cs_100km', 'ns_cs_100km', 'both_cs_100km',
'only_state_cs_100km', 'only_ns_cs_100km', 'cs_100km', 'intensity_100km',
'os_intensity_100km', 'state_intensity_100km', 'ns_intensity_100km'] %}

{% set columns_ny = ['intensity_25km_ny', 'intensity_50km_ny']%}

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
    {% for column in columns %}
    coalesce(cs.{{column}}, 0) as {{column}},
    {% endfor %}
    {% for column in columns_ny %}
    coalesce(csl.{{column}}, 0) as {{column}},
    {% endfor %}
    coalesce(csl.intensity_100km_ny, 0) as intensity_100km_ny
from cluster_years cy
left join {{ ref('cluster_year_stats_all') }} cs
    using(cid, year)
left join {{ ref('cluster_year_stats_lead_all') }} csl
    using(cid, year)