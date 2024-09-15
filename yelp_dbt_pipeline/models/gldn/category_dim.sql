{{config(materialized='table')}}

select
    row_number() over(order by category) as category_id,
    category as name
from (select distinct category from {{ source("stg","business_category") }})



