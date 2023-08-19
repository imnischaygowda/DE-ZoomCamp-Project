


import os
from pathlib import Path # dealing with file paths.
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket # import Gcs block
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0,1) > 0:
    #     raise Exception    
    
    df = pd.read_csv(dataset_url)
    return df
    
@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    out_dir = f"data/{color}"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    # Corrected - Uploading to GCS Does not put the file inside a folder but instead the file is named ‘data\yellow\...’
    path = Path(f"{out_dir}\{dataset_file}.parquet").as_posix()
    df.to_parquet(path, compression='gzip') # pyarrow needs o be installed
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquest to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp")
    # To correct timeout error. timeout=120sec
    # The default timeout is 60 seconds. Timeout may vary depending on your internet speed.
    gcs_block.upload_from_path(from_path=f'{path}', to_path=path, timeout=1200)
    return
    
@flow()
def etl_to_gcs(color) -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    print(dataset_url)
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == "__main__":
    etl_to_gcs()