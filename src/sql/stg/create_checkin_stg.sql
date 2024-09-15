create table if not exists stg.checkin (
    business_id varchar
    checkin_date timestamp
);

copy stg.checkin
from 's3://yelp-project/flattened_data/checkin.snappy.parquet'
iam_role 'Enter IAMROLE creds here'
format AS PARQUET;