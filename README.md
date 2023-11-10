# yelp-etl

Extract & transform (with compute provided by Spark) the [Yelp Academic Dataset](https://www.yelp.com/dataset/documentation/main) in an [Apache Iceberg](https://iceberg.apache.org/docs/latest/spark-writes/) data lake (with object storage provided by Minio).

## Quickstart (MacOS)

*You'll need [Docker Desktop for MacOS.](https://docs.docker.com/desktop/install/mac-install/) installed and running. Have >=4 cores and >=16g RAM available.*

Initialize data lake buckets & prefixes in Minio.

```zsh
mkdir -p data/warehouse/lake
mkdir -p data/yelp/json
mkdir -p data/etl/spark/yelp-etl
mkdir -p data/log/spark/spark-logs
```

Download data from Yelp instructions.

```zsh
open https://www.yelp.com/dataset/download
# Accept conditions & download the JSON dataset as a .zip to ~/Downloads
tar -xvf ~/Downloads/yelp_dataset.tar -C ./data/yelp/json
```

Create Python app deployment files.

```zsh
cp app.py data/etl/spark/yelp-etl/
zip -vr data/etl/spark/yelp-etl/yelp_etl.zip yelp_etl/*
```

Run Spark Master + Worker and Minio (S3) locally.

```zsh
docker-compose up --build -d
```

Observe ETL in the data lake with UIs.

```zsh
open http://localhost:9001/ # minio console
open http://localhost:8080/ # spark master
open http://localhost:8081/ # spark worker
open http://localhost:4040/ # spark jobs
```

Run all pipelines with spark-submit.

```zsh
# Get an interactive bash shell
docker exec -it spark-master /bin/bash
```

```bash
sh run-all-pipelines.sh
```

Cleanup.

```bash
exit
docker-compose down -v
```

## Running Pipelines

Get help by executing:

```bash
python app.py --help
```

Extraction pipelines `--pipeline extract`

```bash
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
```

Cleaning pipelines `--pipeline clean`

```bash
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
```

Enrichment pipelines `--pipeline enrich`

```bash
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
```

TODO: Add gold pipeline examples.

## Contributing

You will need Python ~3.8.1 with Poetry ~1.6.0 installed.

Install Python app & dependencies.

```zsh
poetry install --include dev
```

Run tests.

```zsh
poetry run python -m unittest
```

Format & lint with `pre-commit`.

```zsh
poetry run pre-commit install
# Format & lint happen automatically on commit.
```

## Development Environment (MacOS)

*An opinionated guide to set up your local environment.*

Install [brew](https://github.com/Homebrew/brew).

```zsh
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Install Python3.8 with [pyenv](https://github.com/pyenv/pyenv). Install [poetry](https://github.com/python-poetry/poetry).

```zsh
brew update
brew install pyenv
pyenv init >> ~/.zshrc
exec zsh -l
pyenv install 3.8
pyenv local 3.8
curl -sSL https://install.python-poetry.org | python3 -
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
mkdir -p ~/.zfunc
poetry completions zsh > ~/.zfunc/_poetry
exec zsh -l
poetry config virtualenvs.prefer-active-python true
poetry config virtualenvs.in-project true
```

## References

- [criteo/cluster-pack](https://github.com/criteo/cluster-pack/blob/master/examples/spark-with-S3/docker-compose.yml/)
- [tabular-io/iceberg-rest-image](https://github.com/tabular-io/iceberg-rest-image/)
- [tabular-io/docker-spark-iceberg](https://github.com/tabular-io/docker-spark-iceberg/)
- [Spark and Iceberg Quickstart](https://iceberg.apache.org/spark-quickstart/#spark-and-iceberg-quickstart)
- [Yelp Dataset Docs](https://www.yelp.com/dataset/documentation/main/)
