from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''

Main Module Name - stage_redshift.py

Sub Module Name - N/A

Sub Module Name Description - Overloads the Class StageToRedshiftOperator with the redshift connection that loads data from AWS S3 Bucket to Redshift

Variables Details  - 
redshift_conn_id   - Redshift connection id from the Airflow WebUI for running the PostgresHook hooks.
table_name  	   - Name of the staging table to be loaded
aws_credentials_id - AWS Credentials ID setup via Airflow WebUI
s3_bucket          - Path of the AWS S3 Bucket where source files reside
s3_key	           - AWS S3 Key
extra_param	   - Usuall used for specify the FORMAT option for JSON file format        
region 		   - Area/Region/country where the S3 files are residing on the AWS servers        


'''

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)


# Copy SQL template for the S3 COPY FROM command
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS JSON '{}'      
    """

# Applying the Default Arguements
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket= "",
                 s3_key = "",
                 extra_param="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.extra_param = extra_param
        self.region = region

# Execution Block - Stage_Redshift Operator
def execute(self, context):
	
# Fetching the AWS Credetentials from Airflow Vault

        aws_hook = AwsHook(self.aws_credentials_id)
        redentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

# Deleting the Redshift Table before loading the data into it	
        self.log.info("Deleting the data from destination Redshift table")

        redshift.run("DELETE FROM {}".format(self.table))
        
# Preparing the rendered key variable for the S3 Key       
        self.log.info("Preparing the rendered key variable for the S3 Key")
        rendered_key = self.s3_key.format(**context)	

# Preparing the S3 Path
        self.log.info("Preparing the S3 path")	

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

# Calling the COPY SQL command template to load the data into the staging table in the Redshift database	
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.sercret_key,
            self.region,
            self.extra_param
        )

# Copying the data from the S3 Path to the Staging  Table
        self.log.info(f" Copying data from '{s3_path}' to Staging Table ----> '{self.table}'")
        redshift.run(formatted_sql)

# Execution Block ended
        self.log.info("Execution Block ended")



