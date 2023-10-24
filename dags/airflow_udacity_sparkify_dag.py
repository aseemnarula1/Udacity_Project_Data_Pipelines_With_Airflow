from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

print("Importing Airflow Libraries Python Modules")

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

# Preparing the Default Arguements for Airflow module DAG
print("Preparing the Default Arguements for Airflow module DAG")

default_args = {
     'owner': 'udacity_airflow_project_sparkify',
     'start_date': datetime.now(),    
     'retries':3,
     'retry_delay': timedelta(minutes = 5),
     'catchup': False,
     'email_on_retry': False,
     'depends_on_past': False
}

# Defining DAG - airflow_udacity_sparkify_dag
print("Defining DAG - airflow_udacity_sparkify_dag")

dag = DAG('airflow_udacity_sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
	      start_date = datetime.now(),	
          schedule_interval='0 * * * *',
          catchup = False,
          max_active_runs=1  		
        )

# Begining with the Start Operator with the DAG execution
print("Begining with the Start Operator with the DAG execution")

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql',
)


# Stage to RedShift Operator for Staging Events Table
print("Stage to RedShift Operator for Staging Events Table")

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = 'udacity_dend',
    s3_key = 'log_data',
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
    region = 'us-west-2'
)

# Stage to RedShift Operator for Staging Songs Table
print("Stage to RedShift Operator for Staging Songs Table")

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key="song_data/A/A",
    extra_param = "JSON 'auto' ",
    region = 'us-west-2'
)

# Load Fact Operator for SongPlays Table 
print("Load Fact Operator for SongPlays Table")

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table_name="songplays",
    sql_statement=SqlQueries.songplay_table_insert,
    append_data=False
)

# Loading User Dimension Table in RedShift
print("Load User Dimension Table in RedShift")

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table_name="users",
    sql_statement=SqlQueries.user_table_insert,
    append_data=False
)

# Loading Song Dimension Table in RedShift
print("Loading Song Dimension Table in RedShift")

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table_name="songs",
    sql_statement=SqlQueries.song_table_insert,
    append_data=False
)

# Loading Artist Dimension Table in RedShift
print("Loading Artist Dimension Table in RedShift")

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    sql = SqlQueries.time_table_insert,
    append_data=False
)

# Loading Time Dimension Table in RedShift
print("Loading Time Dimension Table in RedShift")

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    sql = SqlQueries.time_table_insert
)

# Running Data Quality Checks

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    data_quality_checks = [
        {'data_check_dq_sql': 'select count(*) from public.songs where title is null', 'dq_expected_value': 0},
        {'data_check_dq_sql': 'select count(*) from public.artists where name is null', 'dq_expected_value': 0 },
        {'data_check_dq_sql': 'select count(*) from public.users where first_name is null', 'dq_expected_value': 0},
        {'data_check_dq_sql': 'select count(*) from public.time where month is null', 'dq_expected_value': 0},
        {'data_check_dq_sql': 'select count(*) from public.songsplay where userid is null', 'dq_expected_value': 0 }
    ]
)

# End Operator to stop the DAG Execution
print("End Operator to stop the DAG Execution")

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG Task Dependency

start_operator  >> stage_events_to_redshift
start_operator  >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table 
stage_songs_to_redshift  >> load_songplays_table

load_songplays_table  >> load_user_dimension_table
load_songplays_table  >> load_song_dimension_table
load_songplays_table  >> load_artist_dimension_table
load_songplays_table  >> load_time_dimension_table

load_user_dimension_table    >> run_quality_checks
load_song_dimension_table    >> run_quality_checks
load_artist_dimension_table  >> run_quality_checks
load_time_dimension_table    >> run_quality_checks

run_quality_checks           >> end_operator
