# pylint: disable=import-error
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

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

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 data_format="",
                 *args, **kwargs):

        """
        :param redshift_conn_id: The redshift connection id. The name or identifier for
        establishing a connection to Redshift.
        :type redshift_conn_id: str
        :param aws_credentials_id: The AWS connection id. The name or identifier fo
        establishing a connection to AWS
        :type aws_credentials_id: str
        :param table: The name of the table where the data should be loadede.
        :type table: str
        :param s3_bucket: The source s3 bucket. This is the bucket where the original data resides.
        :type s3_bucket: str
        :param s3_key: The s3 key. This is the file path for data source.
        :type s3_key: str
        :param data_format: Here we can describe the format of the data using the COPY command syntax
        referenced here: https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html#r_COPY-syntax-overview-data-format
        :type data_format: str
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.data_format = data_format
        self.aws_credentials_id = aws_credentials_id
        #self.delimiter = delimiter
        #self.ignore_headers = ignore_headers
        

    def execute(self, context):
        self.log.info("AWS: {}".format(self.aws_credentials_id))
        aws_hook = AwsHook(self.aws_credentials_id, client_type="redshift")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.data_format
            #self.ignore_headers,
            #self.delimiter
        )
        self.log.info("SQL: ")
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)





