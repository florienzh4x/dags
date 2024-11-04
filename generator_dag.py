from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from generator_utils.generate_folder import GenerateFolder
from generator_utils.generate_content import GenerateContent


folder_generator = GenerateFolder()
file_generator = GenerateContent()

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
    dag_id='generator_task_process',
    default_args=default_args,
    description='Generator Process',
    schedule_interval=None,
    catchup=False,
    tags=['GENERATOR']
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

generate_folder_task = PythonOperator(
    task_id='generate_folder_task',
    dag=dag,
    python_callable=folder_generator.generate
)

generate_file_task = PythonOperator(
    task_id='generate_file_task',
    dag=dag,
    python_callable=file_generator.make
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)


start_task >> generate_folder_task >> generate_file_task >> end_task