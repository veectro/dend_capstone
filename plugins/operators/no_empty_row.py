from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class NoEmptyRowOperator(BaseOperator):
    """
    Checks if given table name has no empty rows.

    :param redshift_conn_id: Redshift connection ID
    :param table_names: list of table to be checked
    """
    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 table_names: List[str],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names

    def execute(self, context):
        self.log.info('Checking if tables has empty row started')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table_name in self.table_names:
            self.log.info(f'Checking if table {table_name} has empty row')
            records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table_name}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed. {table_name} returned no results')
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f'Data quality check failed. {table_name} contained 0 rows')
            self.log.info(f'Data quality on table {table_name} check passed with {records[0][0]} records')
