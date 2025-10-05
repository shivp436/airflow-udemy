from airflow.sdk import dag, task
from time import sleep

@dag
def celery_dag():

    @task
    def task1():
        sleep(5)

    @task(
        queue = "high_cpu"
    )
    def task2():
        sleep(5)
    
    @task(
        queue = "high_cpu"
    )
    def task3():
        sleep(5)

    @task
    def task4():
        sleep(5)

    task1() >> [task2(), task3()] >> task4()

celery_dag()
