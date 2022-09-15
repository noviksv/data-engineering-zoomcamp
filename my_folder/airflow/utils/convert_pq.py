import pandas as pd
import pyarrow as pa
import os
import argparse



def main(params):
    input_file = params.input_file
    output_file = params.output_file
    schema = params.schema


    schema_yellow=pa.schema([
        ('vendorid',pa.int64()),
        ('tpep_pickup_datetime',pa.timestamp('us')),
        ('tpep_dropoff_datetime',pa.timestamp('us')),
        ('passenger_count',pa.float64()),
        ('trip_distance',pa.float64()),
        ('ratecodeid',pa.float64()),
        ('store_and_fwd_flag',pa.string()),
        ('pulocationid',pa.int64()),
        ('dolocationid',pa.int64()),
        ('payment_type',pa.int64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('congestion_surcharge',pa.float64()),
        ('airport_fee',pa.float64())
        ])
    schema_green=pa.schema([
        ('vendorid',pa.int64()),
        ('lpep_pickup_datetime',pa.timestamp('us')),
        ('lpep_dropoff_datetime',pa.timestamp('us')),
        ('store_and_fwd_flag',pa.string()),
        ('ratecodeid',pa.float64()),
        ('pulocationid',pa.int64()),
        ('dolocationid',pa.int64()),
        ('passenger_count',pa.float64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.float64()),
        ('congestion_surcharge',pa.float64())
        ])
    schema_fhv=pa.schema([
        ('dispatching_base_num',pa.string()),
        ('pickup_datetime',pa.timestamp('ms')),
        ('dropoff_datetime',pa.timestamp('ms')),
        ('pulocationid',pa.int64()),
        ('dolocationid',pa.int64()),
        ('sr_flag',pa.float64()),
        ('affiliated_base_number',pa.string())
    ])
    
    COLS={
        'VendorID':'vendorid',
        'RatecodeID':'ratecodeid',
        'PULocationID':'pulocationid',
        'PUlocationID':'pulocationid',
        'DOLocationID':'dolocationid',
        'DOlocationID':'dolocationid',
        'dropOff_datetime':'dropoff_datetime',
        'SR_Flag':'sr_flag',
        'Affiliated_base_number':'affiliated_base_number'
        }

    df = pd.read_parquet(input_file)

    df = df.rename(columns=COLS)
    if schema == 'green':
        df.to_parquet(output_file,schema=schema_green)
    elif schema == 'yellow':
        df.to_parquet(output_file,schema=schema_yellow)
    elif schema == 'fhv':
        df.to_parquet(output_file,schema=schema_fhv)
    else:
        df.to_parquet(output_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Standartize parquet files with schema')

    parser.add_argument('--input_file', required=True, help='name of the input file')
    parser.add_argument('--output_file', required=True, help='name of the output file')
    parser.add_argument('--schema', required=False, help='name of schema definition')
    args = parser.parse_args()

    main(args)