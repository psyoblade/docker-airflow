from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Seoul")


args = {
    'owner': 'psyoblade',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 10, tzinfo=local_tz),
    'retries': 1,
    'catchup': False
    }

dag = DAG(
    dag_id='bash-op',
    default_args=args,
    schedule_interval='* * * * *'
    )


t1 = BashOperator(task_id='print_date',
                    bash_command='date',
                    dag=dag)

t2 = BashOperator(task_id='sleep',
                    bash_command='sleep 3',
                    dag=dag)

t3 = BashOperator(task_id='print_whoami',
                    bash_command='whoami',
                    dag=dag)

t1 >> t2 >> t3
