{{ config(materialized='table') }}

select 
    review_id,
    business_id,
    user_id,
    date_id,
    stars,
    case when useful < 0 then 0 else useful end as useful,
    case when funny < 0 then 0 else funny end as funny,
    case when cool < 0 then 0 else cool end as cool,
    review_text
from {{ source("stg","review") }} as r
join {{ ref("date_dim") }} as d 
    on cast(to_char(r.review_date,'YYYYMMDD') as integer) = d.date_id
