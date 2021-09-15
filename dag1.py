from datetime import datetime, timedelta
from airflow import DAG
from repo.operator import ReportPodOperator
# /opt/airflow/dags/repo/
import time
from airflow.utils.dates import days_ago


default_args = {
    "owner": "lzhaoxue",
    "depends_on_past": False,
    "email": ["li.zhaoxue@anghami.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    'Automation-p1-data-load',
    default_args=default_args,
    description='Pre royalties calculation, loading plays data and premium revenue',
    # schedule_interval=timedelta(days=1),
    schedule_interval = '0 2 2 * *', # 2nd day of each month 
    catchup = False,
    start_date = datetime(2021, 9, 2),
    end_date = datetime(2021, 12, 1)
)

cmd = ['python']

t1 = ReportPodOperator(
    dag=dag,
    name='test1',
    task_id='task1test',
    args=['/reporting_sys/scripts/premiumSubs.py'],
    cmds=cmd,
    is_delete_operator_pod=True
)

t1 
