from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

'''

Main Module Name - airflow_udacity_sparkify_dag.py

Module Name Description - This DAG calls the Airflow Operators to load the data from S3 bucket to Redshift tables followed by
                          loading fact and dimension tables by calling sub-modules.

Sub Module Name - load_dimension.py, load_fact.py,data_quality.py, stage_redshift.py

Sub Module Name Description - Overloads the Class StageToRedshiftOperator with the redshift connection for first deleting 
                              and then inserting the data into the staging table   		
Variables Details - N/A



'''



#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

# Setting the default arguements 
default_args = {
    'owner': 'udacity-data-engineer-nanodegree',
    'start_date': datetime.now(),
    'depends_on_past': False,       
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'schedule_interval': '@hourly'
}

# Defining the DAG configuration
dag = DAG('airflow_udacity_sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
        )

# Defining variables 
staging_songs_table  = "staging_songs"
staging_events_table = "staging_events"
fact_songplays_table = "songplays"
dim_users_table      = "users"
dim_time_table       = "time"
dim_artists_table    = "artists"
dim_songs_table      = "songs"

# Start Operator for DAG
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Defining the S3 Bucket and S3 Key for the Log Data to be loaded in the staging events table
s3_bucket="udacity-dend"
s3_key="log_data"                          

# Defining the params variable values for the staging events table loading, extra params are used as a FORMAT JSON in this case
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events_S3_to_Redshift',
    dag=dag,
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    redshift_table_name=staging_events_table,
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
)



# Defining the S3 Key for the Log Data to be loaded in the staging songs table
s3_key="song_data/A/A/B"                                    
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs_S3_to_Redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    redshift_table_name=staging_songs_table,
    extra_params="REGION 'us-west-2'  FORMAT as JSON 'auto' TRUNCATECOLUMNS BLANKSASNULL  EMPTYASNULL"    
)

# Defining variables values for loading Songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_creds_id="aws_credentials",
    table_name="songplays",
    sql_statement=SqlQueries.songplay_table_insert,
    append_data=False

)

# Loading the Users Dimension Table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_creds_id="aws_credentials",
    table_name="users",
    sql_statement=SqlQueries.user_table_insert,
    append_data=False
)

# Loading the Songs Dimension Table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_creds_id="aws_credentials",
    table_name="songs",
    sql_statement=SqlQueries.song_table_insert,
    append_data=False
)

# Loading the Artists Dimension Table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_creds_id="aws_credentials",
    table_name = "artists",
    sql_statement = SqlQueries.artist_table_insert,
    append_data=False
)

# Loading the Time Dimension Table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_creds_id="aws_credentials",
    table_name = "time",
    sql_statement = SqlQueries.time_table_insert,
    append_data=False
)


# Running Data Quality Checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_creds_id="aws_credentials",
    input_staging_songs_table = staging_songs_table,
    input_staging_events_table = staging_events_table,
    input_fact_songplays_table = fact_songplays_table,
    input_dim_users_table      = dim_users_table,
    input_dim_time_table       = dim_time_table,
    input_dim_artists_table    = dim_artists_table,
    input_dim_songs_table      = dim_songs_table,
    data_quality_checks=
        [       
        {'dq_check_sql': 'select count(*) from songs where title is null', 'expected_value': 0},
        {'dq_check_sql': 'select count(*) from artists where name is null', 'expected_value': 0 },
        {'dq_check_sql': 'select count(*) from users where first_name is null', 'expected_value': 0},
        {'dq_check_sql': 'select count(*) from time where month is null', 'expected_value': 0},
        {'dq_check_sql': 'select count(*) from songsplay where userid is null', 'expected_value': 0 }
        ]
)


# End Operator
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