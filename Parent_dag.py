from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import boto3


def create_batches(**kwargs):
    aws_hook = AwsHook(aws_conn_id='aws_default')
    credentials = aws_hook.get_credentials()

    s3 = boto3.client('s3', aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key)
    s3_prefix = s3.list_objects_v2(Bucket='newdta', Prefix='csv')


default_args = {
    'start_date': datetime(2023, 5, 22),
    'schedule_interval': None,
}

with DAG('ParentDAG1', start_date=datetime(2023, 5, 22), schedule_interval=None) as dag:
    Pass_prefix = TriggerDagRunOperator(
        task_id='Pass_prefix',
        trigger_dag_id='ChildDAG1',
        conf={'prefix': 'csv', 'batch_start': 0, 'batch_end': 100}
    )

Pass_prefix