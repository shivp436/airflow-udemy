from airflow.sdk import dag, task 
from airflow.sdk.bases.sensor import PokeReturnValue

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # there is no postgres write operator, so we use hook to do it
# from airflow.providers.standard.operators.python import PythonOperator

import requests
import csv
from datetime import datetime
import random
api_path = 'https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json'

'''
def _extract_user(ti):
    fake_user = ti.xcom_pull(task_ids='fetch_user_data')
    # response = requests.get(api_path)
    # fake_user = response.json()
    # print(fake_user)
    return {
        'id': fake_user['id'],
        'firstname': fake_user['personalInfo']['firstName'],
        'lastname': fake_user['personalInfo']['lastName'],
        'email': fake_user['personalInfo']['email']
    }
'''

@dag
def user_processing():
    # Operator (Task)
    create_user_table = SQLExecuteQueryOperator(
        task_id = 'create_user_table',
        conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                firstname VARCHAR(255),
                lastname VARCHAR(255),
                email VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''
    )

    @task.sensor(poke_interval=30, timeout=300)
    def fetch_user_data() -> PokeReturnValue:
        response = requests.get(api_path)
        # print(response.status_code)
        if response.status_code == 200:
            condition = True 
            fake_user = response.json()
        else:
            condition = False 
            fake_user = None 
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    # Old Method
    '''
    extract_user = PythonOperator(
        task_id = 'extract_user',
        python_callable = _extract_user
    )
    '''

    # New Method: try to use @task decorator for PythonOperators,
    # Question: Why to prefer one over the other? 
    # Question: should we use operators as much as possible or decorators
    @task
    def extract_user(fake_user):
        return {
            # 'id': fake_user['id'],
            'id': int(random.random() * 10000),
            'firstname': fake_user['personalInfo']['firstName'],
            'lastname': fake_user['personalInfo']['lastName'],
            'email': fake_user['personalInfo']['email']
        }

    @task
    # Question: Any operator to input into csv file directly from dict?
    def process_user(user_info):
        '''
        user_info = {
            'id': '1234',
            'firstname': 'john',
            'lastname': 'doe',
            'email': 'hello@gmail.com'
        }
        '''
        user_info['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open('/tmp/user_info.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    @task
    # Question: can I use the SQLExecuteQueryOperator for this insert? from csv file to Table
    # If no, why? If yes, how?
    def store_user():
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.copy_expert(
            sql='COPY users FROM STDIN WITH CSV HEADER',
            filename = '/tmp/user_info.csv'
        )

    '''
    fake_user = fetch_user_data() # we need to call this function as we are using a decorator for it
    user_info = extract_user(fake_user)
    process_user(user_info)
    store_user()
    '''

    # process_user(extract_user(create_user_table >> fetch_user_data())) >> store_user()
    create_user_table >> process_user(extract_user(fetch_user_data())) >> store_user()

user_processing()
