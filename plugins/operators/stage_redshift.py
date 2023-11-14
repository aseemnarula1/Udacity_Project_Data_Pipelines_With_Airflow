from airflow.contrib.hooks.aws_hook import AwsHook 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''

Main Module Name - stage_redshift.py

Sub Module Name - N/A

Sub Module Name Description - Overloads the Class StageToRedshiftOperator with the redshift connection for first deleting 
                              and then inserting the data into the staging table   		
Variables Details - 

redshift_conn_id  - Redshift connection id from the Airflow WebUI for running the PostgresHook hooks.
aws_creds_id      - AWS Credentials stored with ACCESS KEY ID and SECRET ACCESS KEY ID 
s3_bucket         - Name of the S3 Bucket
s3_key            - sub folder location/name 
table_name  	  - Name of the dimension table name
extra_params 	  - Usual DELETE/INSERT SQL statement for loading the staging data table


'''

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'  
        {}
    """
# Applying Default Arguments
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_creds_id="aws_credentials",
                 redshift_table_name="",
                 s3_bucket="udacity-dend",
                 s3_key="",
                 extra_params="",
                 *args,
                 **kwargs
                ):
# Initializing the parameters with the self operator instance
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id    =   redshift_conn_id
        self.aws_creds_id        =   aws_creds_id
        self.redshift_table_name =   redshift_table_name
        self.s3_bucket           =   s3_bucket
        self.s3_key              =   s3_key
        self.extra_params        =   extra_params

# Execution block - Stage To Redshift Operator
    def execute(self, context):
        self.log.info('Stage RedShift module started')

        aws_hook    =   AwsHook(self.aws_creds_id)
        self.log.info('Gettting AWS Credentials from the Airflow WebUI AWS Hook')

        credentials =   aws_hook.get_credentials()
        redshift    =   PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from target staging Redshift table before loading the data")
        redshift.run("DELETE FROM {}".format(self.redshift_table_name))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f'S3 path: {s3_path}')
        
        self.log.info('Formatting SQL to be used in the StageToRedshiftOperator')
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.redshift_table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.extra_params

        )

        self.log.info(formatted_sql)
        redshift.run(formatted_sql)
        self.log.info('Formatting SQL running completed')


