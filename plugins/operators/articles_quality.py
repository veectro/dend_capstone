from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class ArticlesQualityOperator(BaseOperator):
    """
    Check if articles table has the same amount as the staging_articles table.

    :param redshift_conn_id: Redshift connection ID
    :param table_names: list of table to be checked
    """
    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Checking if tables article has same amount like staging_articles')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # check if staging_articles has same amount as article
        staging_articles_count = redshift_hook.get_records("SELECT COUNT(*) FROM staging_articles")[0][0]
        articles_count = redshift_hook.get_records("SELECT COUNT(*) FROM dim_articles")[0][0]

        if staging_articles_count != articles_count:
            raise ValueError(
                f"Amount of staging_articles ({staging_articles_count}) is not equal to articles ({articles_count})")
        else:
            self.log.info(
                f"Amount of staging_articles ({staging_articles_count}) is equal to articles ({articles_count})")
