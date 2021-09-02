from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Load Dimension Operator: Take data from S3 and copy them into Redshift 
    """
    
    # UI colour in Apache Airflow
    ui_color = '#358140'
    
    # SQL statement to copy data
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}' 
        JSON '{}'
    """

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 json_sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_sql = json_sql
        

    def execute(self, context):
        # Set credentials
        self.log.info("----> Setting credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Copy data from S3 to redshift
        self.log.info(f"Copying data from S3 (file: {self.s3_key}) to Redshift (table destination: {self.target_table})")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.json_sql
        )
        
#         self.log.info(formatted_sql)

        # Run query
        redshift.run(formatted_sql)





