#!/bin/bash

# Source -> Bronze pipeline jobs

/opt/spark/bin/spark-submit \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name user \
    --bucket_col user_id

/opt/spark/bin/spark-submit \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name business \
    --bucket_col business_id

/opt/spark/bin/spark-submit \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name review \
    --bucket_col business_id

/opt/spark/bin/spark-submit \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name checkin \
    --bucket_col business_id

/opt/spark/bin/spark-submit \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name tip \
    --bucket_col business_id

# Bronze -> Silver pipeline jobs
/opt/spark/bin/spark-submit \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline silver \
    --namespace yelp

# TODO: Silver -> Gold pipeline jobs
