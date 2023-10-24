from airflow.hooks.postgres_hook import PostgresHook
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

    @apply_defaults
    def __init__(self,
                 data_quality_checks = [],
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.data_quality_checks = data_quality_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):

        redshift = PostgresHook(self.redshift_conn_id)
        count_error_check  = 0
        
        for dq_check_step in self.data_quality_checks:
            
            dq_check_query     = dq_check_step.get('data_check_dq_sql')
            dq_expected_result = dq_check_step.get('dq_expected_value')
            
            dq_output_result = redshift.get_records(dq_check_query)[0]
            
            self.log.info(f"Running Data Quality query   : {dq_check_query}")
            self.log.info(f"Expected Data Quality result : {dq_expected_result}")
            self.log.info(f"Check Data Quality result    : {dq_output_result}")
            
            
            if dq_output_result[0] != dq_expected_result:
                count_error_check += 1
                self.log.info(f" DAG Data quality check fails at the step   : {dq_check_query}")
                
            
        if count_error_check > 0:
            self.log.info('DAG Data Quality checks are failed')
	        
        else:
            self.log.info('DAG Data Quality checks are passed')
	   