from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():

    @task
    def push_xcom() -> Dict[str, Any]:
        value = 42
        text = 'Hello World!'
        # context['ti'].xcom_push( key = 'my_key', value = value)
        # return is equivalent of xcom_push
        return {
            'value': value,
            'text': text
        }

    @task
    def pull_xcom(data: Dict[str, Any]) -> Dict[str, Any]:
        # val = context['ti'].xcom_pull(task_ids='t1', key='my_key')
        return {
            'value': data['value']*2,
            'text': data['text'] + ' Again!'
        }

    val = push_xcom()
    pull_xcom(val)

xcom_dag()
