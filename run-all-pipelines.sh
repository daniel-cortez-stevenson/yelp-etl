#!/bin/bash

# Source -> Bronze pipeline jobs

/opt/spark/bin/spark-submit \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=4g \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=4g \
    --conf spark.sql.shuffle.partitions=8 \
    --num-executors=2 \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --buckets 8 \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name user \
    --bucket_col user_id

/opt/spark/bin/spark-submit \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=4g \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=4g \
    --conf spark.sql.shuffle.partitions=8 \
    --num-executors=2 \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --buckets 8 \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name business \
    --bucket_col business_id

/opt/spark/bin/spark-submit \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=4g \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=4g \
    --conf spark.sql.shuffle.partitions=8 \
    --num-executors=2 \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --buckets 8 \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name review \
    --bucket_col business_id

/opt/spark/bin/spark-submit \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=4g \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=4g \
    --conf spark.sql.shuffle.partitions=8 \
    --num-executors=2 \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --buckets 8 \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name checkin \
    --bucket_col business_id

/opt/spark/bin/spark-submit \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=4g \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=4g \
    --conf spark.sql.shuffle.partitions=8 \
    --num-executors=2 \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline bronze \
    --buckets 8 \
    --input_path s3a://yelp/json \
    --namespace yelp \
    --table_name tip \
    --bucket_col business_id

# Bronze -> Silver pipeline jobs
/opt/spark/bin/spark-submit \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=4g \
    --conf spark.driver.memoryOverheadFactor=0.2 \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=4g \
    --conf spark.executor.memoryOverheadFactor=0.2 \
    --conf spark.sql.shuffle.partitions=8 \
    --num-executors=2 \
    --py-files s3a://etl/spark/yelp-etl/yelp_etl.zip \
    s3a://etl/spark/yelp-etl/app.py \
    --pipeline silver \
    --buckets 8 \
    --namespace yelp

# TODO: Silver -> Gold pipeline jobs
