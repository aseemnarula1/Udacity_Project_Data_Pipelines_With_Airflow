from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''

Main Module Name - data_quality.py

Sub Module Name - N/A

Sub Module Name Description - Overloads the Class DataQualityOperator with the redshift connection for checking and iterating on the records for data quality checks

Variables Details - 

redshift_conn_id    - Redshift connection id from the Airflow WebUI for running the PostgresHook hooks.
data_quality_checks - Data Quality checks variable for holding results - expected results vs output results

'''


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

# Applying Default Arguments
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_creds_id="",                       
                 input_staging_songs_table = "",
                 input_staging_events_table = "",
                 input_fact_songplays_table = "",
                 input_dim_users_table      = "",
                 input_dim_time_table       = "",
                 input_dim_artists_table    = "",
                 input_dim_songs_table      = "",
                 data_quality_checks = [],
                 *args, **kwargs):
        
# Initializing the parameters with the self operator instance
        super(DataQualityOperator, self).__init__(*args, **kwargs)       
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds_id = aws_creds_id
        self.input_staging_songs_table = input_staging_songs_table
        self.input_staging_events_table = input_staging_events_table
        self.input_fact_songplays_table = input_fact_songplays_table
        self.input_dim_users_table = input_dim_users_table
        self.input_dim_time_table = input_dim_time_table
        self.input_dim_artists_table = input_dim_artists_table
        self.input_dim_songs_table = input_dim_songs_table
        self.data_quality_checks = data_quality_checks

# Execution block - Data Quality Operator
    def execute(self, context):

        aws_hook    =   AwsHook(self.aws_creds_id)
        self.log.info('Getting AWS Credentials from the Airflow WebUI AWS Hook')

        redshift_hook    =   PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Running the Data Quality Checks')
        input_staging_songs_table_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_staging_songs_table}")
        input_staging_events_table_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_staging_events_table}")
        input_fact_songplays_table_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_fact_songplays_table}")
        input_dim_users_table_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_dim_users_table}")
        input_dim_time_table_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_dim_time_table}")
        input_dim_artists_table_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_dim_artists_table}")
        input_dim_songs_table_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_dim_songs_table}")
  
        self.log.info(f"Data quality checks on table {self.input_staging_songs_table} ---> {input_staging_songs_table_records} records") 
        self.log.info(f"Data quality checks on table {self.input_staging_events_table} ---> {input_staging_events_table_records} records") 
        self.log.info(f"Data quality checks on table {self.input_fact_songplays_table} ---> {input_fact_songplays_table_records} records") 
        self.log.info(f"Data quality checks on table {self.input_dim_users_table} ---> {input_dim_users_table_records} records")
        self.log.info(f"Data quality checks on table {self.input_dim_time_table} ---> {input_dim_time_table_records} records")
        self.log.info(f"Data quality checks on table {self.input_dim_artists_table} ---> {input_dim_artists_table_records} records")
        self.log.info(f"Data quality checks on table {self.input_dim_songs_table} ---> {input_dim_songs_table_records} records")

        if len(input_staging_songs_table_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_staging_songs_table} returned no results")
        
        if len(input_staging_events_table_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_staging_events_table} returned no results")
        
        if len(input_fact_songplays_table_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_fact_songplays_table} returned no results")
        
        if len(input_dim_users_table_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_dim_users_table} returned no results")
        
        if len(input_dim_time_table_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_dim_time_table} returned no results")
        
        if len(input_dim_songs_table_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_dim_songs_table} returned no results")
        
        if len(input_dim_artists_table_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_dim_artists_table} returned no results")
        
        count_error  = 0
        
        for check_step in self.data_quality_checks:
            
            dq_check_query     = check_step.get('dq_check_sql')
            expected_result = check_step.get('expected_value')
            
            result = redshift_hook.get_records(dq_check_query)[0]
            
            self.log.info(f"Running DQ query   : {dq_check_query}")
            self.log.info(f"Expected DQ result : {expected_result}")
            self.log.info(f"Compare result    : {result}")
            
            
            if result[0] != expected_result:
                count_error += 1
                self.log.info(f"Data quality check fails At   : {dq_check_query}")
                
            
        if count_error > 0:
            self.log.info('DQ checks - Failed')
        else:
            self.log.info('DQ checks - Passed')