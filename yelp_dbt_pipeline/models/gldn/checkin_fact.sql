{{ config(materialized='table') }}

select 
    row_number() over() as checkin_id,
    business_id,
    date_id
from {{ source("stg","checkin") }} as c
join {{ ref("date_dim") }} as d
    on cast(to_char(c.checkin_date,'YYYYMMDD') as integer) = d.date_id
