create table if not exists stg.tip (
user_id varchar,
business_id varchar,
tip_text varchar(65535),
tip_date timestamp,
compliment_count integer
);

copy stg.tip
from 's3://yelp-project/flattened_data/tip.snappy.parquet'
iam_role 'Enter IAMROLE creds here'
format AS PARQUET;