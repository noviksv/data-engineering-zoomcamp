
#run postgres
docker run -it \
-e POSTGRES_USER=root \
-e POSTGRES_PASSWORD=root \
-e POSTGRES_DB=ny_taxi \
-p 5432:5432 \
-v /Users/sergeynovik/etl/learning/data-engineering-zoomcamp/my_folder/ny_taxi_postgres_data:/var/lib/postgresql/data:rw \
postgres:13

#run ingestion script
URL='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost  \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_tripdata_trip \
    --url=$URL

#dockerize container
docker build -t ingest:v1 .
#run pg database 
docker-compose up
#run ingestion with h=network form docker-compose
URL='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
docker run -it  \
    --network=my_folder_default \
    ingest:v1  \
    --user=root \
    --password=root \
    --host=pgdatabase  \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_tripdata_trip \
    --url=$URL



dataset_url="https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet"
dataset_file="${dataset_url##*/}"
path_to_local_file="/tmp/"
path_to_creds="/Users/sergeynovik/etl/learning/data-engineering-zoomcamp/my_folder/de-zoomcamp-411.json"
BUCKET=dtc_data_lake_de-zoomcamp-411 

dataset_url=${dataset_url}
dataset_file=${dataset_file}
path_to_local_file=${path_to_local_file}
path_to_creds=${path_to_creds}

curl -sS "$dataset_url" > $path_to_local_file/$dataset_file
gcloud auth activate-service-account --key-file=$path_to_creds
gsutil -m cp $path_to_local_file/$dataset_file gs://$BUCKET