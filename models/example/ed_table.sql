{{ config(materialized='view') }}

with ed_table as (
    select * from {{ source('my_source', 'main_table') }} where year > 1989
)

select * from ed_table