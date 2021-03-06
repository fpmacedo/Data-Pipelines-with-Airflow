# pylint: disable=import-error
from datetime import datetime, timedelta
import os
from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from helpers.sql_queries import SqlQueries
from airflow.models import Variable

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Filipe',
    'start_date': datetime(2021, 5, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('My Dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log-data",
    data_format="JSON 's3://udacity-dend/log_json_path.json'"#.format(s3_bucket, s3_log_json_path_key),
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song-data",
    data_format="JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    insert_query=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    insert_query=SqlQueries.user_table_insert,
    truncate_insert=True,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    insert_query=SqlQueries.song_table_insert,
    truncate_insert=True,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    insert_query=SqlQueries.artist_table_insert,
    truncate_insert=True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    insert_query=SqlQueries.time_table_insert,
    truncate_insert=True,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays","users","songs","artists","time"],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>stage_events_to_redshift
start_operator>>stage_songs_to_redshift

stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table

load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table

load_user_dimension_table>>run_quality_checks
load_song_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks

run_quality_checks>>end_operator