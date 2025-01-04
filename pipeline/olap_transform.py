from datetime import timedelta, datetime
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="final-dwh"
BUCKET_NAME = 'chinook_bucket'
STAGING_DATASET = "chinook_stagging"
DATASET = "chinook_olap"
LOCATION = "asia-southeast1"

default_args = {
    'owner': 'Khanh Linh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

with DAG('ChinookOLAP', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
    
    create_D_Table = DummyOperator(
        task_id = 'Create_D_Table',
        dag = dag
        )

# create olap tables
    create_dim_location = BigQueryOperator(
        task_id = 'create_dim_location',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Dim_Location.sql'
        )

    create_dim_time = BigQueryOperator(
        task_id = 'create_dim_time',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Dim_Time.sql'
        )   

    create_dim_track = BigQueryOperator(
        task_id = 'create_dim_track',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Dim_Track.sql'
        )

    create_fact_sales = BigQueryOperator(
        task_id = 'create_fact_sales',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Fact_Sales.sql'
        )

# view data in fact
    check_fact_sales = BigQueryCheckOperator(
        task_id = 'check_fact_sales',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.fact_sales`'
        ) 

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
start_pipeline >> create_D_Table

create_D_Table >> [create_dim_location, create_dim_track, create_dim_time]

[create_dim_location, create_dim_track, create_dim_time] >> create_fact_sales

create_fact_sales >> check_fact_sales

check_fact_sales >> finish_pipeline