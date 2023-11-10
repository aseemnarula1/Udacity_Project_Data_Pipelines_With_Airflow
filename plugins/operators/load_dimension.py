from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''

Main Module Name - load_dimension.py

Sub Module Name - N/A

Sub Module Name Description - Overloads the Class LoadDimensionOperator with the redshift connection for first deleting 
                              and then inserting the data into the dimension table   		

Variables Details - 

redshift_conn_id  - Redshift connection id from the Airflow WebUI for running the PostgresHook hooks.
table_name  	  - Name of the dimension table name
sql_statement 	  - Usual DELETE/INSERT SQL statement for loading the dimension data table
append_data       - Flag used to check the append data into the dimension table before inserting the data

'''


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

# Applying Default Arguments
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_creds_id = "",
                 table_name="",
                 sql_statement="",
                 append_data=False,
                 *args, **kwargs):

# Initializing the parameters with the self operator instance
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds_id = aws_creds_id
        self.table_name = table_name
        self.sql_statement = sql_statement
        self.append_data = append_data

# Execution block
    def execute(self, context):	
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Adding data to {self.table_name} dimention table.')
        if self.append_data != True:
            redshift_hook.run(f"DELETE FROM {self.table_name}")
        redshift_hook.run(f"""INSERT INTO {self.table_name} 
                              {self.sql_statement} ;""")
        self.log.info('Dimension table {self.table_name} loaded')
