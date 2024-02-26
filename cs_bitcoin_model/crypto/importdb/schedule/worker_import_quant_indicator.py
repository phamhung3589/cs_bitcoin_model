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
target_date = '{{ (execution_date + macros.timedelta(days=0)).strftime("%Y%m%d") }}'


def get_command_import_quant_indicator():
    command = 'cd {} && /home/ecosystem/miniconda3/envs/hungph/bin/python -m  crypto.importdb.crypto_quant.import_crypto_quant_indicator --date {}'.format(
    project_dir, target_date)

    return command


dag = DAG('import_quant_indicator',
          schedule_interval='0 1 * * *',
          default_args=default_args)

t1 = BashOperator(
    task_id='run_import_cryptoquant_indicator',
    bash_command=get_command_import_quant_indicator(),
    email_on_retry=True,
    dag=dag)
