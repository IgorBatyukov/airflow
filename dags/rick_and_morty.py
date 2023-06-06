"""
Igor Batyukov

Load RaM top locations with max number of residents to GreenPlum

"""

from airflow import DAG
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from plugins.rick_and_morty_operator import IBatjukov10RamLocationsOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 1),
    'end_date': datetime(2022, 7, 30),
    'owner': 'Igor Batyukov',
    'retries': 1,
    'poke_interval': 600
}

csv_path = '/tmp/ram.csv'

with DAG("rick_and_morty",
         schedule_interval='@weekly',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['rick_and_morty']
         ) as dag:

    load_top_locations_to_csv = IBatjukov10RamLocationsOperator(
        task_id='load_top_locations_to_csv',
        num_of_locations=3,
        execution_dt='{{ ds }}'
    )


    def load_csv_to_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='postgres_pod')
        pg_hook.run("""
                    CREATE TABLE IF NOT EXISTS ram (dt varchar, 
                                                    id varchar, 
                                                    name varchar, 
                                                    type varchar, 
                                                    dimension varchar,
                                                    residents varchar)
                    """, False)
        pg_hook.copy_expert("COPY ram FROM STDIN DELIMITER ',';", csv_path)


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    remove_csv = BashOperator(
        task_id='remove_csv',
        bash_command=f'rm {csv_path}'
    )
    load_top_locations_to_csv >> load_csv_to_gp >> remove_csv
