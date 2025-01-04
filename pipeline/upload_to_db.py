from datetime import timedelta, datetime


from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator



GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="final-dwh"
GS_PATH = "sales/"
BUCKET_NAME = 'chinook_bucket'
STAGING_DATASET = "chinook_stagging"
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

with DAG('csv_to_oltp', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )


    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
        )    
# create table
    load_dataset_artist = GCSToBigQueryOperator(
        task_id = 'load_dataset_artist',
        bucket = BUCKET_NAME,
        source_objects = ['data/Artist.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.artist',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'ArtistId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
        )
    load_dataset_album = GCSToBigQueryOperator(
        task_id = 'load_dataset_album',
        bucket = BUCKET_NAME,
        source_objects = ['data/Album.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.album',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'AlbumId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ArtistId', 'type': 'INTEGER', 'mode': 'NULLABLE'}
            ]
        )
    load_dataset_playlist = GCSToBigQueryOperator(
        task_id = 'load_dataset_playlist',
        bucket = BUCKET_NAME,
        source_objects = ['data/Playlist.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.playlist',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'PlaylistId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
        )
    load_dataset_mediatype = GCSToBigQueryOperator(
        task_id = 'load_dataset_mediatype',
        bucket = BUCKET_NAME,
        source_objects = ['data/MediaType.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.mediaType',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'MediaTypeId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
        )
    load_dataset_genre = GCSToBigQueryOperator(
        task_id = 'load_dataset_genre',
        bucket = BUCKET_NAME,
        source_objects = ['data/Genre.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.genre',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'GenreId', 'type': 'INTEGER', 'mode': 'REQUIRED'}
            ]
        )
    load_dataset_track = GCSToBigQueryOperator(
        task_id = 'load_dataset_track',
        bucket = BUCKET_NAME,
        source_objects = ['data/Track.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.track',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'TrackId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AlbumId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'MediaTypeId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'GenreId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Composer', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Miliseconds', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Bytes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'UnitPrices', 'type': 'FLOAT', 'mode': 'REQUIRED'}
            ]
        )
    load_dataset_playlistTrack = GCSToBigQueryOperator(
        task_id = 'load_dataset_playlistTrack',
        bucket = BUCKET_NAME,
        source_objects = ['data/PlaylistTrack.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.playlistTrack',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'PlaylistId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'TrackId', 'type': 'INTEGER', 'mode': 'REQUIRED'}
            ]
        )
    load_dataset_employee = GCSToBigQueryOperator(
        task_id = 'load_dataset_employee',
        bucket = BUCKET_NAME,
        source_objects = ['data/Employee.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.employee',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'EmployeeId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ReportsTo', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'BirthDate', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'HireDate', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'Address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'PostalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Phone', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Fax', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
        )
    load_dataset_customer = GCSToBigQueryOperator(
        task_id = 'load_dataset_customer',
        bucket = BUCKET_NAME,
        source_objects = ['data/Customer.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.customer',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'CustomerId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Company', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'PostalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Phone', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Fax', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'SupportRepId', 'type': 'INTEGER', 'mode': 'NULLABLE'}
            ]
        )     
    load_dataset_invoice = GCSToBigQueryOperator(
        task_id = 'load_dataset_invoice',
        bucket = BUCKET_NAME,
        source_objects = ['data/Invoice.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.invoice',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'InvoiceId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'CustomerId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'InvoiceDate', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'BillingAddress', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BillingCity', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BillingState', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BillingCountry', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BillingPostalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Total', 'type': 'FLOAT', 'mode': 'NULLABLE'}
            ]
        )
    load_dataset_invoiceLine = GCSToBigQueryOperator(
        task_id = 'load_dataset_invoiceLine',
        bucket = BUCKET_NAME,
        source_objects = ['data/InvoiceLine.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.invoiceLine',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'InvoiceLineId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'InvoiceId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'TrackId', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'UnitPrice', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'Quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'}
            ]
        )
  
  # view data in each table
    check_dataset_artist = BigQueryCheckOperator(
        task_id = 'check_dataset_artist',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.artist`'
        )

    check_dataset_album = BigQueryCheckOperator(
        task_id = 'check_dataset_album',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.album`'
        )

    check_dataset_playlist = BigQueryCheckOperator(
        task_id = 'check_dataset_playlist',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.playlist`'
        ) 

    check_dataset_playlistTrack = BigQueryCheckOperator(
        task_id = 'check_dataset_playlistTrack',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.playlistTrack`'
        )

    check_dataset_mediaType = BigQueryCheckOperator(
        task_id = 'check_dataset_mediaType',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.mediaType`'
        )               

    check_dataset_genre = BigQueryCheckOperator(
        task_id = 'check_dataset_genre',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.genre`'
        ) 
    check_dataset_customer = BigQueryCheckOperator(
        task_id = 'check_dataset_customer',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.customer`'
        ) 
    check_dataset_track = BigQueryCheckOperator(
        task_id = 'check_dataset_track',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.track`'
        ) 
    check_dataset_employee = BigQueryCheckOperator(
        task_id = 'check_dataset_employee',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.employee`'
        ) 
    check_dataset_invoice = BigQueryCheckOperator(
        task_id = 'check_dataset_invoice',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.invoice`'
        ) 
    check_dataset_invoiceLine = BigQueryCheckOperator(
        task_id = 'check_dataset_invoiceLine',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.invoiceLine`'
        ) 

    create_D_Table = DummyOperator(
        task_id = 'Create_D_Table',
        dag = dag
        )
    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
start_pipeline >> load_staging_dataset

load_staging_dataset >> [load_dataset_artist, load_dataset_album, load_dataset_playlist, load_dataset_mediatype, load_dataset_genre, load_dataset_track, load_dataset_playlistTrack, load_dataset_employee, load_dataset_customer, load_dataset_invoice, load_dataset_invoiceLine]

load_dataset_artist >> check_dataset_artist
load_dataset_album >> check_dataset_album
load_dataset_playlist >> check_dataset_playlist
load_dataset_mediatype >> check_dataset_mediaType
load_dataset_genre >> check_dataset_genre
load_dataset_track >> check_dataset_track
load_dataset_playlistTrack >> check_dataset_playlistTrack
load_dataset_employee >> check_dataset_employee
load_dataset_customer >> check_dataset_customer
load_dataset_invoice >> check_dataset_invoice
load_dataset_invoiceLine >> check_dataset_invoiceLine

[check_dataset_artist, check_dataset_album, check_dataset_playlist, check_dataset_mediaType, check_dataset_genre, check_dataset_track, check_dataset_playlistTrack, check_dataset_employee, check_dataset_customer, check_dataset_invoice, check_dataset_invoiceLine] >> finish_pipeline