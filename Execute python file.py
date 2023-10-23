import boto3
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.dummy_operator import DummyOperator
import logging

# Set the name of the S3 bucket where the Python script is stored

s3_bucket = 'pyhonscript-bucket'
s3_key = 'data/'

def execute_script():
    session = boto3.Session()
    credentials = session.get_credentials()

    s3_hook = S3Hook()
    file_contents = s3_hook.read_key(key='s3://pyhonscript-bucket/data/batches_python.py')
    exec(file_contents)


# Set default arguments for the DAG

default_args = {
    'owner': 'akash_harde',
    'start_date': datetime(2023, 5, 2),

}

dag = DAG(
    'execute_script_from_s3_bucket',
    default_args=default_args,
    schedule_interval=None
)

start = DummyOperator(task_id='start',dag=dag)


#  S3 key sensor to wait for the script file to be added to the bucket
wait_for_file = S3KeySensor(
    task_id='wait_for_file',
    bucket_name=s3_bucket,
    bucket_key=s3_key,
    timeout=6,
    poke_interval=1,
    dag=dag
)
# Python operator to execute the script
execute_script = PythonOperator(
    task_id='execute_script',
    python_callable=execute_script,
    dag=dag
)

end = DummyOperator(task_id='end',dag=dag)


start >> wait_for_file >> execute_script >> end
