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

def get_command_update_model():
    command = 'cd {} && /home/ecosystem/miniconda3/envs/hungph/bin/python -m  crypto.importdb.btc_prediction.update_model'.format(
    project_dir)

    return command


dag = DAG('update_model',
          schedule_interval='30 1 * * *',
          default_args=default_args)

t1 = BashOperator(
    task_id='update_model_btc_prediction',
    bash_command=get_command_update_model(),
    email_on_retry=True,
    dag=dag)
