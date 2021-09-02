from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, EmptyQualityOperator)

from helpers import SqlQueries

"""
Create the Dag and its default settings
"""

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None # Only when asked
        )

"""
Define all the operator and tasks used for this pipeline
"""

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


# Take the raw logs in S3 and copy them into redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_sql="s3://udacity-dend/log_json_path.json"
)


# Take the raw songs data from S3 and copy them into redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_sql="auto"
)


# Use the staging raw data in redshift (songs and logs) to create the fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table="songplays",
    sql_querry=SqlQueries.songplay_table_insert
)



# Use the taging raw data in redshift (songs and logs) to create 4 dimensions tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="users",
    sql_querry=SqlQueries.user_table_insert
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="songs",
    sql_querry=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="artists",
    sql_querry=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="time",
    sql_querry=SqlQueries.time_table_insert
)


# Run Quality check to ensure that all the data are correct
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    checks=[{'sql_check':'SELECT COUNT(*) FROM artists WHERE artistid is null', 'expect_value':0}]
)

run_quality_empty = EmptyQualityOperator(
    task_id='Run_empty_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['artists', 'time', 'users', 'songs', 'songplays']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag, trigger_rule='all_done')


"""
Pipeline stages and order
"""

# Step 1: Beggin Dag & load data from logs and songs
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

# Step 2: Create Fact table based on logs and songs
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

# Step 3: Create dimension tables
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]

# Step 4: Check data quality of the imported data
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]   >> run_quality_checks
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]   >> run_quality_empty

# Step 5: Close dags
[run_quality_checks, run_quality_empty] >> end_operator