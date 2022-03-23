from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.stage_image_data import StageImageDataOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.articles_quality import ArticlesQualityOperator
from operators.no_empty_row import NoEmptyRowOperator
from operators.unique_column import UniqueColumnOperator
from helpers import SqlQueries

SOURCE_BUCKET = 'veectro-udacity-capstone'
DOCUMENTATION_MD = """
## recommendation_dwh_dag

This DAG load the data from the S3 bucket and then load the data into the Redshift cluster in staging tables.
From the staging tables, the data is loaded into the fact and dimension tables.

### Input
S3 Buckets :   
  
- `s3://veectro-udacity-capstone`

### Output
Redshift Tables :   
  
- `staging_articles`
- `staging_customers`
- `staging_transactions`
- `articles`
- `customers`
- `transactions`
- `images`
"""

default_args = {
    'owner': 'veectro',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('recommendation_dwh_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False,
          doc_md=DOCUMENTATION_MD
          )

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

# Using PostgresOperator to create staging tables from sql file in dags/sql
# Based on :
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/operators/postgres_operator_howto_guide.html
create_tables_in_redshift = PostgresOperator(
    task_id='create_tables_in_redshift',
    dag=dag,
    postgres_conn_id='redshift_conn_id',
    sql='sql/create_tables.sql'
)

stage_image_data = StageImageDataOperator(
    task_id='stage_image_data',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    aws_conn_id='aws_conn_id',
    s3_bucket=SOURCE_BUCKET,
    s3_key='images/',
    target_table='staging_images'
)

stage_articles_to_redshift = StageToRedshiftOperator(
    task_id='stage_articles_to_redshift',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    s3_bucket=SOURCE_BUCKET,
    s3_key='articles.csv',
    target_table='staging_articles',
    redshift_arn=Variable.get('redshift_iam_arn')
)

stage_customers_to_redshift = StageToRedshiftOperator(
    dag=dag,
    task_id='stage_customers_to_redshift',
    redshift_conn_id='redshift_conn_id',
    s3_bucket=SOURCE_BUCKET,
    s3_key='customers.csv',
    target_table='staging_customers',
    redshift_arn=Variable.get('redshift_iam_arn')
)

stage_transactions_to_redshift = StageToRedshiftOperator(
    dag=dag,
    task_id='stage_transactions_to_redshift',
    redshift_conn_id='redshift_conn_id',
    s3_bucket=SOURCE_BUCKET,
    s3_key='transactions_train.csv',
    target_table='staging_transactions',
    redshift_arn=Variable.get('redshift_iam_arn')
)

load_transactions_table = LoadFactOperator(
    dag=dag,
    task_id='load_transactions_table',
    redshift_conn_id='redshift_conn_id',
    table_name='fact_transactions',
    query=SqlQueries.transactions_table_insert,
)

load_articles_dimension_table = LoadDimensionOperator(
    dag=dag,
    task_id='load_articles_dimension_table',
    redshift_conn_id='redshift_conn_id',
    table_name='dim_articles',
    query=SqlQueries.articles_table_insert,
    append_only=False
)

load_customers_dimension_table = LoadDimensionOperator(
    dag=dag,
    task_id='load_customers_dimension_table',
    redshift_conn_id='redshift_conn_id',
    table_name='dim_customers',
    query=SqlQueries.customers_table_insert,
)

articles_quality = ArticlesQualityOperator(
    dag=dag,
    task_id='check_articles_quality',
    redshift_conn_id='redshift_conn_id',
)

no_empty_row = NoEmptyRowOperator(
    dag=dag,
    task_id='check_no_empty_row',
    redshift_conn_id='redshift_conn_id',
    table_names=['staging_articles', 'staging_customers', 'staging_transactions', 'staging_images',
                 'dim_articles', 'dim_customers', 'fact_transactions']
)

unique_columns = UniqueColumnOperator(
    dag=dag,
    task_id='check_unique_columns',
    redshift_conn_id='redshift_conn_id',
    table_tuples=[('dim_articles', 'article_id'),
                  ('dim_customers', 'customer_id')]
)

star_schema_done = DummyOperator(task_id='star_schema_created', dag=dag)

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

start_operator \
    >> create_tables_in_redshift \
    >> [stage_transactions_to_redshift, stage_articles_to_redshift, stage_customers_to_redshift, stage_image_data] \
    >> load_transactions_table \
    >> [load_articles_dimension_table, load_customers_dimension_table] \
    >> star_schema_done \
    >> [no_empty_row, articles_quality, unique_columns] \
    >> end_operator
