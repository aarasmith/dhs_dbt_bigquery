


{% set columns = get_conflict_vars() %}

    select
        cid,
        {% for column in columns %}
        {{column}} as {{column}}_50km,
        {% endfor %}
        year
    from {{ ref('cluster_stats') }}
    where dist = 50
