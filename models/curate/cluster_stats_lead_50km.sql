{% set prefix_columns = get_lead_vars() %}

    select
        cid,
        {% for column in prefix_columns %}
        {{column}} as {{column}}_50km_ny,
        {% endfor %}
        year - 1 as year
    from {{ ref('cluster_stats') }}
    where dist = 50
