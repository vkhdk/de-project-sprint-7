import airflow
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import date, datetime
import environment_settings as env

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'


default_args = {
                'owner': f'{env.username}',
                'start_date':datetime(2024, 8, 1),
                'retries': 0
                }

with DAG(
    'spark_dag_s7',
    default_args = default_args,
    schedule_interval = None,
    max_active_runs = 1,
    catchup = False) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Task 1: Load local city geo data to HDFS
    load_city_geo = BashOperator(
        task_id = 'load_city_geo',
        dag = dag,
        bash_command = f'spark-submit --master yarn ' \
                       f'{env.scripts_local_path}/load_city_geo.py ' \
                       f'{env.cities_geo_local_path} '\
                       f'{env.ods__cities_geo__path}'
        )
    
    # Task 2: Load events and define city
    load_stg_geo_events_cities_geo = BashOperator(
        task_id = 'load_stg_geo_events_cities_geo',
        dag = dag,
        bash_command = f'spark-submit --master yarn ' \
                       f'{env.scripts_local_path}/load_stg_geo_events_cities_geo.py ' \
                       f'2022-03-01 '\
                       f'45 ' \
                       f'{env.source__geo_events__path} '\
                       f'{env.ods__cities_geo__path} '\
                       f'{env.stg__geo_events_cities_geo__path} '\
                       f'0.1'
        )

    # Task 3: Calculate users area dm
    calc_users_area_dm = BashOperator(
        task_id = 'calc_users_area_dm',
        dag = dag,
        bash_command = f'spark-submit --master yarn ' \
                       f'{env.scripts_local_path}/calculate_users_area_dm.py ' \
                       f'{env.stg__geo_events_cities_geo__path} '\
                       f'{env.dm__users_area__path} '
        )

    # Task 4: Calculate geo area dm
    calc_geo_area_dm = BashOperator(
        task_id = 'calc_geo_area_dm',
        dag = dag,
        bash_command = f'spark-submit --master yarn ' \
                       f'{env.scripts_local_path}/calculate_geo_area_dm.py ' \
                       f'{env.stg__geo_events_cities_geo__path} '\
                       f'{env.dm__geo_area__path} '
        )    
    
    # Task 5: Calculate users friends recommendation dm
    calc_users_friends_recom_dm = BashOperator(
        task_id = 'calc_users_friends_recom_dm',
        dag = dag,
        bash_command = f'spark-submit --master yarn ' \
                       f'{env.scripts_local_path}/calculate_users_friends_recommendation_dm.py ' \
                       f'{env.stg__geo_events_cities_geo__path} '\
                       f'{env.dm__users_friends_recommendation__path} '
        )    
start >> load_city_geo >> load_stg_geo_events_cities_geo >> [calc_users_area_dm, calc_geo_area_dm, calc_users_friends_recom_dm] >> end
    

