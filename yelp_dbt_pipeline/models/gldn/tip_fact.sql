{{ config(materialized='table') }}

select 
    row_number() over() as tip_id,
    business_id,
    user_id,
    date_id,
    compliment_count
from {{ source("stg","tip") }} as t
join {{ ref("date_dim") }} as d
    on cast(to_char(t.tip_date,'YYYYMMDD') as integer) = d.date_id