from airflow.sdk import dag, task

@dag
def sql_decorator_dag():

    @task.sql(
        conn_id = 'postgres'
    )
    def get_xcoms():
        return 'SELECT * FROM xcom'
    
    get_xcoms()

sql_decorator_dag()
