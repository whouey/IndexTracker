from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.macros import ds_format
from datetime import datetime, timedelta
import logging

def tw_year(ds):
    return datetime.strptime(ds, '%Y-%m-%d').year - 1911

def check_s3_key(aws_conn_id, bucket, key, true_path, false_path):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3 = S3Hook(aws_conn_id = aws_conn_id)
    if s3.check_for_key(key=key, bucket_name=bucket):
        return true_path
    else:
        return false_path

def download_file_to_s3(aws_conn_id, bucket, key, url, expected_content_type=None):
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

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 9),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "workflow",
    default_args=default_args,
    schedule_interval='0 10 * * *',
    catchup=True,
    user_defined_macros={
        'tw_year': tw_year,
    }
) as dag:

    check_schedule_file = BranchPythonOperator(
        task_id='check_schedule_file',
        python_callable=check_s3_key,
        op_kwargs={
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
            'key': 'tw/holidaySchedule_{{ tw_year(ds) }}.csv',
            'true_path': 'schedule_file_existed',
            'false_path': 'download_schedule_file',
        },
    )

    download_schedule_file = PythonOperator(
        task_id='download_schedule_file',
        python_callable=download_file_to_s3,
        op_kwargs={
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
            'key': 'tw/holidaySchedule_{{ tw_year(ds) }}.csv',
            'url': 'https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=csv&queryYear={{ tw_year(ds) }}'
        }
    )

    schedule_file_existed = DummyOperator(
        task_id = 'schedule_file_existed'
    )

    check_schedule = ShortCircuitOperator(
        task_id='check_schedule',
        provide_context=True,
        python_callable=check_is_trading_day,
        trigger_rule='none_failed_or_skipped',
        op_kwargs={
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
            'key': 'tw/holidaySchedule_{{ tw_year(ds) }}.csv',
        }
    )


    futures_s3_tamplate = 'tw/raw/futures/{}'
    futures_dealt_filename = 'Daily_{{ macros.ds_format(ds, "%Y-%m-%d", "%Y_%m_%d") }}.zip'
    futures_dealt_url_template = 'https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/{}'
    futures_spread_filename = 'Daily_{{ macros.ds_format(ds, "%Y-%m-%d", "%Y_%m_%d") }}_C.zip'
    futures_spread_url_template = 'https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV_C/{}'

    options_s3_template = 'tw/raw/options/{}'
    options_dealt_filename = 'OptionsDaily_{{ macros.ds_format(ds, "%Y-%m-%d", "%Y_%m_%d") }}.zip'
    options_dealt_url_template = 'https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/{}'

    check_futures_dealt_file = BranchPythonOperator(
        task_id='check_futures_dealt_file',
        python_callable=check_s3_key,
        op_kwargs={
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
            'key': futures_s3_tamplate.format(futures_dealt_filename),
            'true_path': 'futures_dealt_file_existed',
            'false_path': 'download_futures_dealt_file',
        },
    )

    download_futures_dealt_file = PythonOperator(
        task_id='download_futures_dealt_file',
        python_callable=download_file_to_s3,
        op_kwargs={
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
            'key': futures_s3_tamplate.format(futures_dealt_filename),
            'url': futures_dealt_url_template.format(futures_dealt_filename),
            'expected_content_type': 'application/zip'
        }
    )

    futures_dealt_file_existed = DummyOperator(
        task_id = 'futures_dealt_file_existed'
    )

    check_futures_spread_file = BranchPythonOperator(
        task_id='check_futures_spread_file',
        python_callable=check_s3_key,
        op_kwargs={
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
            'key': futures_s3_tamplate.format(futures_spread_filename),
            'true_path': 'futures_spread_file_existed',
            'false_path': 'download_futures_spread_file',
        },
    )

    download_futures_spread_file = PythonOperator(
        task_id='download_futures_spread_file',
        python_callable=download_file_to_s3,
        op_kwargs={
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
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
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
            'key': options_s3_template.format(options_dealt_filename),
            'true_path': 'options_dealt_file_existed',
            'false_path': 'download_options_dealt_file',
        },
    )

    download_options_dealt_file = PythonOperator(
        task_id='download_options_dealt_file',
        python_callable=download_file_to_s3,
        op_kwargs={
            'aws_conn_id': 'aws_credentials',
            'bucket': 'indextracker',
            'key': options_s3_template.format(options_dealt_filename), 
            'url': options_dealt_url_template.format(options_dealt_filename), 
            'expected_content_type': 'application/zip'
        }
    )

    options_dealt_file_existed = DummyOperator(
        task_id = 'options_dealt_file_existed'
    )

    raw_file_backup_checked = DummyOperator(
        task_id = 'raw_file_backup_checked',
        trigger_rule='all_done',
    )


    check_schedule_file >> [ download_schedule_file, schedule_file_existed ] >> check_schedule
    check_schedule >> [ check_futures_dealt_file, check_futures_spread_file, check_options_dealt_file ]
    check_futures_dealt_file >> [ download_futures_dealt_file, futures_dealt_file_existed ] >> raw_file_backup_checked
    check_futures_spread_file >> [ download_futures_spread_file, futures_spread_file_existed ] >> raw_file_backup_checked
    check_options_dealt_file >> [ download_options_dealt_file, options_dealt_file_existed ] >> raw_file_backup_checked