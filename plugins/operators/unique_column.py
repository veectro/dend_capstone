from typing import List, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class UniqueColumnOperator(BaseOperator):
    """
    This operator checks if a column is unique in a table.

    :param redshift_conn_id: Redshift connection ID
    :param table_tuples: List of tuples containing table name and column name
    """
    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 table_tuples: List[Tuple[str, str]],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_tuples = table_tuples

    def execute(self, context):
        self.log.info('Checking if columns are unique in tables')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table_tuple in self.table_tuples:
            table_name, column_name = table_tuple

            self.log.info(f'Checking if column {column_name} is unique in table {table_name}')
            sql = f"""
                SELECT COUNT(*)
                FROM {table_name}
                GROUP BY {column_name}
                HAVING COUNT(*) > 1
            """
            result = redshift_hook.get_records(sql)
            if result:
                self.log.info(f'Column {column_name} is not unique in table {table_name}')
                raise ValueError(f'Column {column_name} is not unique in table {table_name}')
            else:
                self.log.info(f'Column {column_name} is unique in table {table_name}')
