from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from simulation_utils.simulation_file_action import SimulationAction

simulation_action = SimulationAction()

default_args = { 
    'owner': 'GEN_Z',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout':timedelta(minutes=10),
    'dagrun_timeout': timedelta(minutes=20),
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    dag_id='simulation_task_process',
    default_args=default_args,
    description='Simulator Process',
    schedule_interval=None,
    catchup=False,
    tags=['SIMULATOR']
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

random_pick_file = PythonOperator(
    task_id='random_pick_file',
    dag=dag,
    python_callable=simulation_action.action
)

start_task >> random_pick_file