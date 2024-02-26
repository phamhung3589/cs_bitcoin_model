from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates

default_args = {
    'owner': 'hungph',
    'depends_on_past': False,
    'start_date': dates.days_ago(2),
    'email': ['phamhung3589@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=120),
}

dag = DAG('import_indicator_warning',
          schedule_interval='0 1 * * *',
          default_args=default_args)

project_dir = '/home/ecosystem/data/hungph/csloth-model-creator-v1'
target_dir = '{{ (execution_date - macros.timedelta(days=0)).strftime("%Y_%m_%d") }}'
command = 'cd {} && /home/ecosystem/miniconda3/envs/hungph/bin/python -m  crypto.importdb.import_cryptoquant_to_db --date {}'.format(
    project_dir, target_dir)


operator = BashOperator(
    task_id='run_import_data_to_sql_server',
    bash_command=command,
    email_on_retry=True,
    dag=dag)
