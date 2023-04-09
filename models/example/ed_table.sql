{{
  config(
    materialized = "table",
    indexes=[
        {'columns': ['cid', 'year']}
    ],
    pre_hook = ["set work_mem = '256MB'"],
    post_hook = ["reset work_mem"]
  )
}}

{% set columns = ['id_year', 'id_', 'year', 'CEY', 'circ1', 'circ2', 'ynum', 'v010',
       'v012', 'v025', 'v101', 'v104', 'v107', 'v130', 'v133', 'v149', 'v212',
       'v508', 'v511', 'CLUSTER', 'LATNUM', 'LONGNUM', 'ADM1NAME',
       'v131', 'country', 'iso2c', 'sex', 'dk_dummy', 'sib_count',
       'sib_count_sa', 'brother_count', 'brother_count_sa', 'sister_count',
       'older_brothers', 'older_brothers_sa', 'older_sisters',
       'older_sisters_sa', 'younger_brothers', 'younger_sisters',
       'older_siblings', 'older_siblings_sa', 'younger_siblings',
       'death_count', 'marriage_year_dummy', 'is_married_dummy',
       'no_education', 'dropout1', 'dropout2', 'unknown_location',
       'never_mover', 'before_move', 'sex_num', 'SurveyId'] %}


select 
    {% for column in columns %}
    "{{column}}", --as {{column|lower}},
    {% endfor %}
    concat("SurveyId", "CLUSTER") as cid
from {{ source('my_source', 'main_table') }}
where year > 1989