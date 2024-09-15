create table if not exists stg.review (
    review_id varchar,
    user_id varchar,
    business_id varchar,
    stars integer,
    useful integer,
    funny integer,
    cool integer,
    review_text varchar(65535),
    review_date timestamp
);

copy stg.review
from 's3://yelp-project/flattened_data/review.snappy.parquet'
iam_role 'Enter IAMROLE creds here'
format AS PARQUET;