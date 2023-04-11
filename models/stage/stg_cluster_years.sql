

select
    CONCAT (SurveyId, CLUSTER, '-', v101) as cid,
    SurveyId as survey_id,
    iso2c,
    year,
    v101 as region,
    CLUSTER as cluster,
    LONGNUM as long,
    LATNUM as lat
from {{ source('raw', 'cluster_years') }}