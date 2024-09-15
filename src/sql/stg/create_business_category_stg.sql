-- Workaround for using array + unnest since redshift does not support.
create table if not exists stg.business_category as 
with numbers as (
    select row_number() over(order by true) as n
    from (select true from stg.business limit 25) t

)
select
    distinct
    business_id,
    trim(split_part(categories,',',numbers.n::integer)) as category
from stg.business
cross join numbers
where 
    numbers.n <= regexp_count(categories,',') + 1
    and split_part(categories, ',', numbers.n::integer) != ''