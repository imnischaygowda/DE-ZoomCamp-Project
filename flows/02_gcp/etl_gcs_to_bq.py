
from pathlib import Path # dealing with file paths.
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def ectract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month}.parquet"
    gcs_block = GcsBucket.load("de-zoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BigQuery"""
    
    # getting GCP credentials from Prefect Block. 
    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-creds")
    
    df.to_gbq(
        destination_table="dezoomcamp.external_" + color + "_tripdata",
        project_id="dtc-de-123",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gcs_to_bq(year, color):
    """Main ETL flow to load data into Big Query"""
    color=color
    year=year
    # month=1
    for i in range(12):
        month = '0'+str(i+1)
        month = month[-2:]
        path = ectract_from_gcs(color, year, month)
        df = transform(path)
        
        write_bq(df, color)
     
if __name__ == "__main__":
    etl_gcs_to_bq('2019', 'green')
    # etl_gcs_to_bq('2020', 'green')
    # etl_gcs_to_bq('2019', 'yellow')
    # etl_gcs_to_bq('2020', 'yellow') 
    # etl_gcs_to_bq('2019', 'fhv')
    # etl_gcs_to_bq('2020', 'fhv')