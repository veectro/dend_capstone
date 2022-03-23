from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):
    """
    Loads data from staging tables into dimension tables.

    :param table_name: The name of the table to load data into.
    :param redshift_conn_id: The connection id of the redshift connection.
    :param query: sql select query to load data from staging tables.
    :param append_data: If True, data will be appended to the table. Otherwise, the table will be truncated
            and then appended in to the table.
    """
    ui_color = '#80BD9E'

    def __init__(self,
                 table_name: str,
                 redshift_conn_id: str,
                 query: str,
                 append_only: bool = False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.append_only = append_only

    def execute(self, context):
        self.log.info(f'Loading dimension table into {self.table_name}')

        # Connecting to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_only:
            self.log.info(f'Truncating {self.table_name}')
            # Executing the query
            table_truncate_sql = f"""
                TRUNCATE TABLE {self.table_name}
            """
            redshift_hook.run(table_truncate_sql)
            self.log.info('Successfully truncating dimension table into Redshift')

        self.log.info(f'Appending data to {self.table_name}')
        # Executing the query
        table_insert_sql = f"""
                       INSERT INTO {self.table_name}
                       {self.query}
                   """
        redshift_hook.run(table_insert_sql)
        self.log.info('Successfully appending dimension table into Redshift')
