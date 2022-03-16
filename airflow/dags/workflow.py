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

def tw_year(ds):
    """
    year in the form of ROC era.

    args:
        ds - templated variable 'ds' of airflow
    return:
        year of ROC as int
    """
    return datetime.strptime(ds, '%Y-%m-%d').year - 1911

def ds_underscore(ds):
    """
    ds but separated by underscore

    args:
        ds - templated variable 'ds' of airflow
    return:
        ds in the form of "yyyy_MM_dd" 
    """
    from airflow.macros import ds_format
    return ds_format(ds, "%Y-%m-%d", "%Y_%m_%d")

def check_s3_key(aws_conn_id, bucket, key, true_path, false_path):
    """
    a function called by a branch operator,
    which check a key of s3 existed.

    args: 
        aws_conn_id - the connection variable id of aws
        bucker - target bucket name
        key - target s3 object key
        true_path - the task name should be returned when key existed
        false_path - the task name should be returned when key not existed
    return:
        a path either true_path or false_path
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3 = S3Hook(aws_conn_id = aws_conn_id)
    if s3.check_for_key(key=key, bucket_name=bucket):
        return true_path
    else:
        return false_path

def download_file_to_s3(aws_conn_id, bucket, key, url, expected_content_type=None):
    """
    download and upload a file to S3 on the fly

    args:
        aws_conn_id - the connection variable id of aws
        bucker - target bucket name
        key - target s3 object key
        url - location of target file
        expected_content_type - http content-type, use to validate the result
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.exceptions import AirflowNotFoundException
    import urllib.request

    s3 = S3Hook(aws_conn_id = aws_conn_id)
    with urllib.request.urlopen(url) as response:

        logging.info(f'expected content_type {expected_content_type} and got content_type {response.getheader(name="Content-Type")}')
        if expected_content_type and \
            response.getheader(name="Content-Type") != expected_content_type:
            raise AirflowNotFoundException('Content-Type mismatched.')

        s3.load_file_obj(response, key, bucket)

def check_is_trading_day(aws_conn_id, bucket, key, **context):
    """
    check schedule file on S3 if `ds` in trading day

    args:
        aws_conn_id - the connection variable id of aws
        bucker - target bucket name
        key - target s3 object key
    return:
        a bool value if `ds` is trading day.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import pandas as pd

    ds = datetime.strptime(context['ds'], '%Y-%m-%d')

    s3 = S3Hook(aws_conn_id = aws_conn_id)
    file_content = s3.get_key(key, bucket).get()['Body'].read().decode('big5')
    df = pd.DataFrame([x.split(',') for x in file_content.split('\n')[2:]])

    df = df.iloc[:, [1,4]]
    df.columns = ['date', 'memo']

    df = df[df['date'] == f'\"{ds.month}月{ds.day}日\"']

    if df.shape[0] > 0:
        is_makeup_day = df.iloc[0, 1] == '\"o\"'

        if is_makeup_day:
            logging.info('is not trading day for it is holiday')
        else:
            logging.info('it is make-up day')

        return is_makeup_day
    else:
        return ds.weekday() < 5

def check_emr_cluster(aws_conn_id, cluster_name, true_path, false_path, **context):
    """
    a function called by a branch operator.
    check if a emr cluster existed by name, and return a path.

    args:
        aws_conn_id - the connection variable id of aws
        cluster_name - target cluster name
        true_path - the task name should be returned when key existed
        false_path - the task name should be returned when key not existed
    return:
        a path either true_path or false_path
    """
    from airflow.providers.amazon.aws.hooks.emr import EmrHook

    emr = EmrHook(aws_conn_id = aws_conn_id)
    
    # any state except TERMINATING and TERMINATED and TERMINATED_WITH_ERRORS
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.list_clusters
    job_flow_id = emr.get_cluster_id_by_name(cluster_name, \
                        cluster_states=['STARTING','BOOTSTRAPPING','RUNNING','WAITING'])

    if job_flow_id:
        context['ti'].xcom_push(key='job_flow_id', value=job_flow_id)
        return true_path
    else:
        return false_path

def create_emr_cluster(aws_conn_id, emr_conn_id, job_flow_overrides, **context):
    """
    create a emr cluster

    args:
        aws_conn_id - the connection variable id of aws
        cluster_name - target cluster name
        job_flow_overrides - a dict contains configurations overrides the 'Extra` in emr connection variable 
    return:
        job flow id created
    """
    from airflow.providers.amazon.aws.hooks.emr import EmrHook
    from airflow.exceptions import AirflowException

    emr = EmrHook(aws_conn_id = aws_conn_id, emr_conn_id = emr_conn_id)

    response = emr.create_job_flow(job_flow_overrides)

    if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
        raise AirflowException(f'JobFlow creation failed: {response}')

    job_flow_id = response['JobFlowId']
    context['ti'].xcom_push(key='job_flow_id', value=job_flow_id)

def add_steps_to_emr_cluster(aws_conn_id, job_flow_id, steps):
    """
    add steps a emr cluster

    args:
        aws_conn_id - the connection variable id of aws
        job_flow_id - the id of target emr cluster
        steps - a dict contains the steps to be added
    return:
        the id of last step added, for the use of waited
    """
    from airflow.providers.amazon.aws.hooks.emr import EmrHook
    from airflow.exceptions import AirflowException

    emr = EmrHook(aws_conn_id = aws_conn_id).get_conn()
    
    if not job_flow_id:
        raise AirflowException(f'JobFlow not existed.')

    response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=steps)

    if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
        raise AirflowException(f'Adding steps failed: {response}')

    return response['StepIds'][-1]

def get_job_steps(subject, ds_underscore):
    """
    a helper function to generate steps dict by subject name and ds

    args:
        subject - the name of target subject
        ds_underscore - the ds but should be underscored
    return:
        a dict of steps
    """
    return [
        {
            'Name': f'Run Spark {subject} raw {ds_underscore}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', f'/home/hadoop/{subject}_raw.py', ds_underscore]
            }
        },
        {
            'Name': f'Run Spark {subject} agg {ds_underscore}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', f'/home/hadoop/{subject}_agg.py', ds_underscore]
            }
        }
    ]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 9),
    "email_on_failure": False,
    "email_on_retry": False,
}

APP_NAME = 'indextracker'

with DAG(
    "workflow",
    default_args=default_args,
    schedule_interval='0 10 * * *',
    catchup=True,
    user_defined_macros={
        'tw_year': tw_year,
        'ds_underscore': ds_underscore,
    }
) as dag:

    check_schedule_file = BranchPythonOperator(
        task_id='check_schedule_file',
        python_callable=check_s3_key,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': 'tw/holidaySchedule_{{ tw_year(ds) }}.csv',
            'true_path': 'schedule_file_existed',
            'false_path': 'download_schedule_file',
        },
    )

    download_schedule_file = PythonOperator(
        task_id='download_schedule_file',
        python_callable=download_file_to_s3,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': 'tw/holidaySchedule_{{ tw_year(ds) }}.csv',
            'url': 'https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=csv&queryYear={{ tw_year(ds) }}'
        }
    )

    schedule_file_existed = DummyOperator(
        task_id='schedule_file_existed'
    )

    check_schedule = ShortCircuitOperator(
        task_id='check_schedule',
        python_callable=check_is_trading_day,
        trigger_rule='none_failed_or_skipped',
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': 'tw/holidaySchedule_{{ tw_year(ds) }}.csv',
        }
    )


    futures_s3_tamplate = 'tw/raw/futures/{}'
    futures_dealt_filename = 'Daily_{{ ds_underscore(ds) }}.zip'
    futures_dealt_url_template = 'https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/{}'
    futures_spread_filename = 'Daily_{{ ds_underscore(ds) }}_C.zip'
    futures_spread_url_template = 'https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV_C/{}'

    options_s3_template = 'tw/raw/options/{}'
    options_dealt_filename = 'OptionsDaily_{{ ds_underscore(ds) }}.zip'
    options_dealt_url_template = 'https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/{}'

    check_futures_dealt_file = BranchPythonOperator(
        task_id='check_futures_dealt_file',
        python_callable=check_s3_key,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': futures_s3_tamplate.format(futures_dealt_filename),
            'true_path': 'futures_dealt_file_existed',
            'false_path': 'download_futures_dealt_file',
        },
    )

    download_futures_dealt_file = PythonOperator(
        task_id='download_futures_dealt_file',
        python_callable=download_file_to_s3,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': futures_s3_tamplate.format(futures_dealt_filename),
            'url': futures_dealt_url_template.format(futures_dealt_filename),
            'expected_content_type': 'application/zip'
        }
    )

    futures_dealt_file_existed = DummyOperator(
        task_id='futures_dealt_file_existed'
    )

    check_futures_spread_file = BranchPythonOperator(
        task_id='check_futures_spread_file',
        python_callable=check_s3_key,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': futures_s3_tamplate.format(futures_spread_filename),
            'true_path': 'futures_spread_file_existed',
            'false_path': 'download_futures_spread_file',
        },
    )

    download_futures_spread_file = PythonOperator(
        task_id='download_futures_spread_file',
        python_callable=download_file_to_s3,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': futures_s3_tamplate.format(futures_spread_filename),
            'url': futures_spread_url_template.format(futures_spread_filename),
            'expected_content_type': 'application/zip'
        }
    )

    futures_spread_file_existed = DummyOperator(
        task_id = 'futures_spread_file_existed'
    )

    check_options_dealt_file = BranchPythonOperator(
        task_id='check_options_dealt_file',
        python_callable=check_s3_key,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': options_s3_template.format(options_dealt_filename),
            'true_path': 'options_dealt_file_existed',
            'false_path': 'download_options_dealt_file',
        },
    )

    download_options_dealt_file = PythonOperator(
        task_id='download_options_dealt_file',
        python_callable=download_file_to_s3,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'bucket': APP_NAME,
            'key': options_s3_template.format(options_dealt_filename), 
            'url': options_dealt_url_template.format(options_dealt_filename), 
            'expected_content_type': 'application/zip'
        }
    )

    options_dealt_file_existed = DummyOperator(
        task_id='options_dealt_file_existed'
    )

    raw_file_backup_checked = DummyOperator(
        task_id='raw_file_backup_checked',
        trigger_rule='all_done',
    )

    check_emr_job_flow = BranchPythonOperator(
        task_id='check_emr_job_flow',
        python_callable=check_emr_cluster,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'cluster_name': APP_NAME,
            'true_path': 'job_flow_existed',
            'false_path': 'create_job_flow',
        }
    )

    # prefer PythonOperator + boto3 over EmrCreateJobFlowOperator for more customization
    create_job_flow = PythonOperator(
        task_id='create_job_flow',
        python_callable=create_emr_cluster,
        op_kwargs={
            'aws_conn_id': 'aws_default',
            'emr_conn_id': 'emr_default',
            'job_flow_overrides': {
                'Steps': [
                    {
                        'Name': 'setup - copy files',
                        'ActionOnFailure': 'TERMINATE_CLUSTER',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['aws', 's3', 'cp', '--recursive', "s3://indextracker/emr/scripts/tw/", '/home/hadoop/']
                        }
                    }
                ]
            }
        }
    )

    job_flow_existed = DummyOperator(
        task_id='job_flow_existed'
    )

    job_flow_checked = DummyOperator(
        task_id='job_flow_checked',
        trigger_rule='all_done',
    )
    

    emr_jobs_checked = DummyOperator(
        task_id='emr_jobs_checked',
        trigger_rule='all_done',
    )

    
    workflow_finished = DummyOperator(
        task_id='workflow_finished',
        trigger_rule='all_done',
    )

    for subject in ['index', 'etf', 'futures', 'options']:

        add_steps_task_id = f'add_steps_to_job_flow_{subject}'

        add_steps_to_job_flow = PythonOperator(
            task_id=add_steps_task_id,
            python_callable=add_steps_to_emr_cluster,
            op_kwargs={
                'aws_conn_id': 'aws_default',
                'job_flow_id': '{{ ti.xcom_pull(key="job_flow_id") }}',
                'steps': get_job_steps(subject, '{{ ds_underscore(ds) }}'),
            }
        )

        job_flow_step_sensor = EmrStepSensor(
            task_id=f'watch_steps_{subject}',
            aws_conn_id='aws_default',
            job_flow_id='{{ ti.xcom_pull(key="job_flow_id") }}',
            step_id=f'{{{{ ti.xcom_pull(task_ids=\'{add_steps_task_id}\', key="return_value") }}}}'
        )

        job_flow_checked >> add_steps_to_job_flow >> job_flow_step_sensor >> emr_jobs_checked

        table_name = f'staging_{subject}_{{{{ ds_underscore(ds) }}}}'

        create_staging = PostgresOperator(
            task_id=f'create_staging_{subject}',
            postgres_conn_id='postgres_default',
            sql=create_staging_table[subject].format(table_name=table_name),
        )

        copy_staging = S3ToRedshiftOperator(
            task_id=f'copy_staging_{subject}',
            aws_conn_id='aws_default',
            redshift_conn_id='postgres_default',
            s3_bucket=APP_NAME,
            s3_key=f'tw/{subject}/agg/{{{{ ds_underscore(ds) }}}}',
            schema='PUBLIC',
            table=table_name,
            copy_options=['FORMAT AS PARQUET'],
        )

        quality_check = SQLCheckOperator(
            task_id=f'quality_check_{subject}',
            conn_id='postgres_default',
            sql=data_quality_check[subject].format(table_name=table_name),
        )
        
        load_data = PostgresOperator(
            task_id=f'load_data_{subject}',
            postgres_conn_id='postgres_default',
            sql=f'load_{subject}.sql',
            params={'subject': subject}
        )

        # trigger drop even upstream failed
        drop_staging = PostgresOperator(
            task_id=f'drop_staging_{subject}',
            postgres_conn_id='postgres_default',
            sql=f'DROP TABLE {table_name};',
            trigger_rule="all_done",
        )

        emr_jobs_checked >> create_staging >> copy_staging >> quality_check >> load_data >> drop_staging >> workflow_finished


    check_schedule_file >> [ download_schedule_file, schedule_file_existed ] >> check_schedule
    check_schedule >> [ check_futures_dealt_file, check_futures_spread_file, check_options_dealt_file ]
    check_futures_dealt_file >> [ download_futures_dealt_file, futures_dealt_file_existed ] >> raw_file_backup_checked
    check_futures_spread_file >> [ download_futures_spread_file, futures_spread_file_existed ] >> raw_file_backup_checked
    check_options_dealt_file >> [ download_options_dealt_file, options_dealt_file_existed ] >> raw_file_backup_checked
    raw_file_backup_checked >> check_emr_job_flow >> [ create_job_flow, job_flow_existed ] >> job_flow_checked