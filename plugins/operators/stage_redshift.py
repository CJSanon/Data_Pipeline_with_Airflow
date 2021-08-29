from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
#     template_fields = ("s3_key",)
    
    sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT as json '{}'
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_option="auto",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_option = copy_option

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Deleting data from S3 to Redshift")
        redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        formatted_sql = StageToRedshiftOperator.sql_template.format(
            self.table, 
            s3_path, 
            credentials.access_key, 
            credentials.secret_key, 
            self.copy_option
        )
        redshift.run(formatted_sql)
        self.log.info("Data now staged")