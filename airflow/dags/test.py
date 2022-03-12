from asyncio.log import logger
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from helpers import create_staging_table, data_quality_check
import logging
import sys

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 9),
    "email_on_failure": False,
    "email_on_retry": False,
}

APP_NAME = 'indextracker'

with DAG(
    "test",
    default_args=default_args,
) as dag:

    load_data = PostgresOperator(
        task_id='load_data_index',
        postgres_conn_id='postgres_default',
        sql='load_index.sql',
        params={'table_name': 'staging_index'}
    )