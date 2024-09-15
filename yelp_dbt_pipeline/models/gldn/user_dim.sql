-- models/gldn/user_dim.sql

{{ config(materialized='table') }}

select  
    user_id,
    initcap(trim(name)) as name,
    review_count,
    cast(yelping_since as timestamp) as yelping_since,
    useful,
    funny,
    cool,
    fans,
    average_stars,
    compliment_hot,
    compliment_more,
    compliment_profile,
    compliment_cute,
    compliment_list,
    compliment_note,
    compliment_plain,
    compliment_cool,
    compliment_funny,
    compliment_writer,
    compliment_photos
    friends_count,
    elite_years_count
from {{ source("stg","user") }}