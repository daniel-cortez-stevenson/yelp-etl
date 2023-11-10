#!/bin/bash

# Extraction (bronze) pipeline jobs
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
    --input s3a://yelp/json/yelp_academic_dataset_user.json \
    --output lake.bronze.yelp.user \
    --entity_type user \
    --pipeline extract \
    --bucket_column user_id \
    --buckets 8

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
    --input s3a://yelp/json/yelp_academic_dataset_business.json \
    --output lake.bronze.yelp.business \
    --entity_type business \
    --pipeline extract \
    --bucket_column business_id \
    --buckets 8

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
    --input s3a://yelp/json/yelp_academic_dataset_review.json \
    --output lake.bronze.yelp.review \
    --entity_type review \
    --pipeline extract \
    --bucket_column business_id \
    --buckets 8

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
    --input s3a://yelp/json/yelp_academic_dataset_checkin.json \
    --output lake.bronze.yelp.checkin \
    --entity_type checkin \
    --pipeline extract \
    --bucket_column business_id \
    --buckets 8

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
    --input s3a://yelp/json/yelp_academic_dataset_tip.json \
    --output lake.bronze.yelp.tip \
    --entity_type tip \
    --pipeline extract \
    --bucket_column business_id \
    --buckets 8

# Cleaning (silver) pipeline jobs
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
     --input lake.bronze.yelp.user \
    --output lake.silver.yelp.user \
    --entity_type user \
    --pipeline clean \
    --bucket_column user_id \
    --buckets 8

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
    --input lake.bronze.yelp.business \
    --output lake.silver.yelp.business \
    --entity_type business \
    --pipeline clean \
    --bucket_column business_id \
    --buckets 8

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
    --input lake.bronze.yelp.review \
    --output lake.silver.yelp.review \
    --entity_type review \
    --pipeline clean \
    --partition_column date_year \
    --bucket_column business_id \
    --buckets 8

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
    --input lake.bronze.yelp.checkin \
    --output lake.silver.yelp.checkin \
    --entity_type checkin \
    --pipeline clean \
    --partition_column date_year \
    --bucket_column business_id \
    --buckets 8

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
    --input lake.bronze.yelp.tip \
    --output lake.silver.yelp.tip \
    --entity_type tip \
    --pipeline clean \
    --partition_column date_year \
    --bucket_column business_id \
    --buckets 8

# Enrichment (silver) pipeline jobs
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
    --input lake.silver.yelp.review \
    --output lake.silver.yelp.user_business_review \
    --entity_type review \
    --pipeline enrich \
    --partition_column date_year \
    --bucket_column business_id \
    --buckets 8 \
    --dimension_inputs lake.silver.yelp.business,lake.silver.yelp.user \
    --dimension_entity_types business,user

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
    --input lake.silver.yelp.checkin \
    --output lake.silver.yelp.business_checkin \
    --entity_type checkin \
    --pipeline enrich \
    --partition_column date_year \
    --bucket_column business_id \
    --buckets 8 \
    --dimension_inputs lake.silver.yelp.business \
    --dimension_entity_types business

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
    --input lake.silver.yelp.tip \
    --output lake.silver.yelp.user_business_tip \
    --entity_type tip \
    --pipeline enrich \
    --partition_column date_year \
    --bucket_column business_id \
    --buckets 8 \
    --dimension_inputs lake.silver.yelp.business,lake.silver.yelp.user \
    --dimension_entity_types business,user

# TODO: Silver -> Gold pipeline jobs
