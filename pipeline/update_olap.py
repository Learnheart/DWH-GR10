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

with DAG('ChinookOLAP_Updated', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
# create csd to update and insert new data
    update_D_Table = DummyOperator(
        task_id = 'update_D_Table',
        dag = dag
        )

    scd1_update_dim_location = BigQueryInsertJobOperator(
        task_id='scd1_update_dim_location',
        use_legacy_sql=False,
        location=LOCATION,
        sql='./scd_query/Dim_Location_scd.sql'  
    )
    scd1_update_dim_time = BigQueryInsertJobOperator(
        task_id='scd1_update_dim_time',
        use_legacy_sql=False,
        location=LOCATION,
        sql='./scd_query/Dim_Time_scd.sql'
    )
    scd1_update_dim_track = BigQueryInsertJobOperator(
        task_id='scd1_update_dim_track',
        use_legacy_sql=False,
        location=LOCATION,
        sql='./scd_query/Dim_Track_scd.sql'  
    )
    scd2_update_fact_sales = BigQueryInsertJobOperator(
        task_id='scd2_update_fact_sales',
        use_legacy_sql=False,
        location=LOCATION,
        sql='./scd_query/Fact_Sales_scd.sql'  
    )

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
start_pipeline >> update_D_Table

update_D_Table >> [scd1_update_dim_location, scd1_update_dim_time, scd1_update_dim_track]

[scd1_update_dim_location, scd1_update_dim_time, scd1_update_dim_track] >> scd2_update_fact_sales >> finish_pipeline