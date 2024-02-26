from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates

default_args = {
    'owner': 'hungph',
    'depends_on_past': False,
    'start_date': dates.days_ago(1),
    'email': ['phamhung3589@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=60),
}

project_dir = '/home/ecosystem/data/hungph/csloth-model-creator-v1'
target_date = '{{ (execution_date + macros.timedelta(hours=1)).strftime("%Y_%m_%dT%H:%M:%S") }}'


def get_command_import_twitter_recent_search():
    command = 'cd {} && /home/ecosystem/miniconda3/envs/hungph/bin/python -m  crypto.importdb.twitter.import_twitter_recent_search --date {}'.format(
    project_dir, target_date)

    return command


def get_command_import_twitter_search_week():
    command = 'cd {} && /home/ecosystem/miniconda3/envs/hungph/bin/python -m  crypto.importdb.twitter.import_twitter_search_week --date {}'.format(
        project_dir, target_date)

    return command


def get_command_import_twitter_advanced_filter_daily():
    command = 'cd {} && /home/ecosystem/miniconda3/envs/hungph/bin/python -m  crypto.importdb.twitter.import_twitter_advanced_search_daily --date {}'.format(
        project_dir, target_date)

    return command


def get_command_import_twitter_advanced_filter_weekly():
    command = 'cd {} && /home/ecosystem/miniconda3/envs/hungph/bin/python -m  crypto.importdb.twitter.import_twitter_advanced_search_weekly --date {}'.format(
        project_dir, target_date)

    return command


dag = DAG('import_twitter_search_trending',
          schedule_interval='0 * * * *',
          default_args=default_args)

t1 = BashOperator(
    task_id='run_import_data_twitter_recent_search',
    bash_command=get_command_import_twitter_recent_search(),
    email_on_retry=True,
    dag=dag)


t2 = BashOperator(
    task_id='run_import_data_twitter_search_week',
    bash_command=get_command_import_twitter_search_week(),
    email_on_retry=True,
    dag=dag)


t3 = BashOperator(
    task_id='run_import_data_twitter_advanced_filter_daily',
    bash_command=get_command_import_twitter_advanced_filter_daily(),
    email_on_retry=True,
    dag=dag)


t4 = BashOperator(
    task_id='run_import_data_twitter_advanced_filter_weekly',
    bash_command=get_command_import_twitter_advanced_filter_weekly(),
    email_on_retry=True,
    dag=dag)