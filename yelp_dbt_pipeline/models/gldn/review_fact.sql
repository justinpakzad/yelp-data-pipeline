{{ config(materialized='table') }}

select 
    review_id,
    business_id,
    user_id,
    date_id,
    stars,
    useful,
    funny,
    cool,
    review_text
from {{ source("stg","review") }} as r
join {{ ref("date_dim") }} as d 
    on cast(to_char(r.review_date,'YYYYMMDD') as integer) = d.date_id
