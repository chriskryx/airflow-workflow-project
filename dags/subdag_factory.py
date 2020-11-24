# subdag_factory.py
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

def subdag_factory(parent_dag_id, subdag_id, default_args) -> DAG:
    with DAG(f'{parent_dag_id}.{subdag_id}', default_args=default_args) as dag:
        for l in ['A', 'B', 'C']:
            DummyOperator(task_id=f'processing_{l}')
    return dag