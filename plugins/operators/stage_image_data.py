from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.baseoperator import BaseOperator


class StageImageDataOperator(BaseOperator):
    """
    Read the given image folder and stage the information into the staging area in redshift.
    The data will only a table with a single column containing the article image id that exists in the folder.

    :param aws_conn_id: AWS connection ID registered in Airflow
    :param redshift_conn_id: Redshift connection ID registered in Airflow
    :param s3_bucket: Source s3 bucket name
    :param s3_key: Source s3 key name (prefix)
    :param target_table: Redshift table name, where the data will be staged
    :param region: AWS region where the s3 bucket is located
    """
    ui_color = '#358140'

    def __init__(self,
                 aws_conn_id: str,
                 redshift_conn_id: str,
                 s3_bucket: str,
                 s3_key: str,
                 target_table: str,
                 region: str = 'us-west-1',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table
        self.region = region

    def execute(self, context):
        self.log.info('Beginning to stage image data to redshift')

        # Load all the image key from s3
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        list_keys = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_key)

        # images/010/0108775015.jpg
        image_ids = ', '.join(['(' + s.split('/')[-1].split('.')[0] + ')' for s in list_keys])  # (1),(2),(3)

        # Connecting to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Save all valid article image id in redshift
        image_id_insert_query = f"""
        INSERT INTO {self.target_table} (article_id)
        VALUES {image_ids}
        """
        redshift_hook.run(image_id_insert_query)
        self.log.info('Successfully stage image data to redshift')
