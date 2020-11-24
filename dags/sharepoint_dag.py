 
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable

# For checking if previous tasks failed and for making tasks fail intentionally
from airflow.exceptions import AirflowFailException

import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 11, 10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    }

dag = DAG(
    'sharepoint_dag',
    default_args=default_args,
    description='Checks the Sharepoint',
    schedule_interval=None,
    dagrun_timeout=timedelta(seconds=5)
)


def get_list_data():
    from shareplum import Site
    from shareplum import Office365
    from shareplum.site import Version

    authcookie = Office365('https://operacrorg.sharepoint.com', username="management@operacrorg.onmicrosoft.com",
                           password="Alsec7374").GetCookies()

    site = Site('https://operacrorg.sharepoint.com', version=Version.v365, authcookie=authcookie)

    sp_list = site.List('Personnel requisition testing')

    data = sp_list.GetListItems('All Items')

    return data


def get_latest_id(**kwargs):

    task_instance = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids='get_list_data')

    d = {}
    for k in data[0]:
        d[k] = tuple(d[k] for d in data)
    return max(d['ID'])


def has_id_updated(**kwargs):

    task_instance = kwargs['task_instance']
    latest_id = task_instance.xcom_pull(task_ids='get_latest_id')

    # Check previous sp list length
    prev_max_id = Variable.get('prev_id')
    if prev_max_id != latest_id:
        print("List has changed:\nThe latest id was %s" % str(prev_max_id) + "\n")
        print("Actual list has the latest id of %s" % str(latest_id))
        return 'upd_list_max_id_var'
    print("Nothing's changed")
    return 'success_task'


def success():
    return None


def upd_list_max_id_var(**kwargs):
    task_instance = kwargs['task_instance']
    latest_id = task_instance.xcom_pull(task_ids='get_latest_id')
    Variable.set("prev_id", latest_id)
    return latest_id


with dag:
      
    getListData = PythonOperator(
        task_id='get_list_data',
        do_xcom_push=True,
        python_callable=get_list_data)

    getLatestId = PythonOperator(
        task_id='get_latest_id',
        do_xcom_push=True,
        provide_context=True,
        python_callable=get_latest_id)

    hasIdUpdtd = BranchPythonOperator(
        task_id='has_id_updated',
        provide_context=True,
        python_callable=has_id_updated)

    updListMaxIdVar = PythonOperator(
        task_id='upd_list_max_id_var',
        provide_context=True,
        python_callable=upd_list_max_id_var)

    successTask = PythonOperator(
        task_id='success_task',
        python_callable=success)

    getListData >> getLatestId >> hasIdUpdtd >> [updListMaxIdVar, successTask]
