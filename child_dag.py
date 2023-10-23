from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
from airflow.models import Variable
import logging
import math

def process_files_batch(batch_index, num_batches, **kwargs):
    s3_prefix = kwargs['dag_run'].conf.get('prefix')
    bucket_name = 'newdta'

    logging.info(f"Processing files with prefix '{s3_prefix}' in batch {batch_index}")

    s3 = boto3.client('s3', aws_access_key_id=Variable.get("aws_access_key_id"),
                             aws_secret_access_key=Variable.get("aws_secret_access_key"))

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    if 'Contents' in response:
        files = [obj['Key'] for obj in response['Contents']]
        total_files = len(files)  # Get the total number of files

        batch_size = math.ceil(total_files / num_batches)  # Calculate the batch size
        batch_start = batch_index * batch_size
        batch_end = min((batch_index + 1) * batch_size, total_files)

        batch_files = files[batch_start:batch_end]  # Files for the current batch
        for file in batch_files:
            # Process each file in the batch
            print(f"Processing file: {file}")
            # Add your file processing logic here
    else:
        total_files = 0
        print(f"No files found with prefix '{s3_prefix}'")

    return total_files

# Define the DAG
with DAG('ChildDAG1', start_date=datetime(2023, 5, 22), schedule_interval=None) as child_dag:

    num_batches = 5  # Number of batches to create

    process_files_batch_tasks = []
    for i in range(num_batches):
        process_files_batch_task = PythonOperator(
            task_id=f'process_files_batch_{i}',
            python_callable=process_files_batch,
            provide_context=True,
            op_kwargs={
                'batch_index': i,
                'num_batches': num_batches,
            }
        )
        process_files_batch_tasks.append(process_files_batch_task)

    process_files_batch_tasks
