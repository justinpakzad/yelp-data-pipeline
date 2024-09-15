create table if not exists stg.user (
    user_id varchar,
    name varchar,
    review_count int,
    yelping_since timestamp,
    useful int,
    funny int,
    cool int,
    fans int,
    average_stars double precision,
    compliment_hot int,
    compliment_more int,
    compliment_profile int,
    compliment_cute int,
    compliment_list int,
    compliment_note int,
    compliment_plain int,
    compliment_cool int,
    compliment_funny int,
    compliment_writer int,
    compliment_photos int,
    friends_count int,
    elite_years_count int
);
copy stg.user
from 's3://yelp-project/flattened_data/user.snappy.parquet'
iam_role 'Enter IAMROLE creds here'
format AS PARQUET;