import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/my-creds-gcp.json'

bucket_name = 'mage-zoomcamp-andrea-gagna-2'
project_id = 'symbolic-bit-411809'

table_name = 'nyc_green_taxi_data'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    print('Writing table to pyarrow')
    table = pa.Table.from_pandas(data)

    print('Creating gcs connection')
    gcs = pa.fs.GcsFileSystem()
    print(gcs)

    print('Writing to dataset')
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
    print('Finished.')

