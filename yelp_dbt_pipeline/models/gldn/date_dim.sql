{{ config(materialized='table') }}

with date_spines as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2004-01-01' as date)",
            end_date="cast('2024-08-01' as date)",
        )


    }}
)
select
    cast(to_char(date_day, 'YYYYMMDD') as integer) as date_id,
    date_day as calendar_date,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    trim(to_char(date_day, 'Day')) as day_of_week,
    trim(to_char(date_day, 'Month')) as month_name,
    case 
        when extract(dow from date_day) in (0, 6) then true 
        else false 
    end as is_weekend
from date_spines
