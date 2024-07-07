from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Função que imprime a data e a hora da execução
def print_execution_date(ds, **kwargs):
    print(f'Data e hora da execução: {ds}')

# Definição da DAG
default_args = {
    'owner': 'Caique Garcia',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'print_execution_date_dag',
    default_args=default_args,
    description='Uma DAG para imprimir a data e a hora da execução',
    schedule_interval=timedelta(days=1),
)

# Tarefa que chama a função de impressão
print_date_task = PythonOperator(
    task_id='print_execution_date_task',
    provide_context=True,
    python_callable=print_execution_date,
    dag=dag,
)
