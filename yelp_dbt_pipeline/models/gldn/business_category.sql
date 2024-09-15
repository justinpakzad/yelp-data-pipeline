{{ config(materialized="table") }}


select  
    bc.business_id,
    cd.category_id
from {{ source("stg","business_category") }} as  bc
join {{ ref("category_dim") }} as cd on bc.category = cd.name