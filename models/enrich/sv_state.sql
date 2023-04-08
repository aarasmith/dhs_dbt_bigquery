


select * from {{ ref('sv_prep') }}
where state = 1
