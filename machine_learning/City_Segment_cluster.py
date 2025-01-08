from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from sklearn.cluster import KMeans
import pandas as pd
from google.cloud import bigquery


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "final-dwh"
BUCKET_NAME = 'chinook_bucket'
DATASET = "chinook_olap"
LOCATION = "asia-southeast1"

default_args = {
    'owner': 'Quynh Trang',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

# Define the function inline
def model(project_id, dataset_id, table_id, num_clusters):
    client = bigquery.Client(project=project_id)

    # Query data for clustering
    query = f"""
        SELECT city_id, frequency, total_price
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE frequency IS NOT NULL AND total_price IS NOT NULL
    """
    query_job = client.query(query)
    results = query_job.result()

    # Load data into a DataFrame
    data = pd.DataFrame([dict(row) for row in results])
    if data.empty:
        raise ValueError("No data returned from query.")

    # Perform clustering
    features = data[['frequency', 'total_price']].values
    kmeans = KMeans(n_clusters=num_clusters, random_state=42)
    data['cluster'] = kmeans.fit_predict(features)

    # Update table with clusters
    temp_table_id = f"{dataset_id}.temp_clusters"
    update_data = data[['city_id', 'cluster']]
    client.load_table_from_dataframe(update_data, temp_table_id).result()

    merge_query = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` AS target
        USING `{project_id}.{temp_table_id}` AS source
        ON target.city_id = source.city_id
        WHEN MATCHED THEN
        UPDATE SET target.cluster = source.cluster
    """
    client.query(merge_query).result()

    # Cleanup
    client.delete_table(temp_table_id, not_found_ok=True)

with DAG('City_Segment_cluster', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(task_id='start_pipeline')

    # Update data table
    update_data = BigQueryOperator(
        task_id='update_data',
        use_legacy_sql=False,
        location=LOCATION,
        sql='./City_Segment.sql',
    )

    # Cluster modeling task
    run_cluster = PythonOperator(
        task_id='run_cluster',
        python_callable=model,
        op_args=['final-dwh', 'chinook_cluster', 'City_Segment', 3]
    )

    finish_pipeline = DummyOperator(task_id='finish_pipeline')

    start_pipeline >> update_data >> run_cluster >> finish_pipeline