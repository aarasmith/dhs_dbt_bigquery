



{% set prefix_columns = ['state_adult_prev_minor',
       'state_adult_prev_major', 'state_child_prev', 'ns_adult_prev_minor',
       'ns_adult_prev_major', 'ns_child_prev', 'both_adult_prev_major',
       'both_adult_prev_minor', 'both_child_prev',
       'only_state_adult_prev_major', 'only_state_adult_prev_minor',
       'only_state_child_prev', 'only_ns_adult_prev_major',
       'only_ns_adult_prev_minor', 'only_ns_child_prev', 'adult_prev_major',
       'adult_prev_minor', 'child_prev', 'state_gs', 'ns_gs', 'both_gs',
       'only_state_gs', 'only_ns_gs', 'gs', 'state_cs', 'ns_cs', 'both_cs',
       'only_state_cs', 'only_ns_cs', 'cs', 'intensity', 'os_intensity',
       'state_intensity', 'ns_intensity'] %}

    select
        cid,
        {% for column in prefix_columns %}
        {{column}} as {{column}}_100km,
        {% endfor %}
        year
    from {{ ref('cluster_stats') }}
    where dist = 100
