from airflow.sdk import dag, task
import random

@dag
def branching():

    @task
    def random_gen() -> int:
        low,high = 0,1000
        return random.randint(low, high)

    @task.branch
    def if_branch(val: int):
        if val%2 == 0:
            return ['even_task', 'common_task']
        return ['odd_task', 'common_task']

    @task
    def odd_task(val: int) -> str:
        return str(val) + ' is odd'

    @task
    def even_task(val: int) -> str:
        return str(val) + ' is even'

    @task
    def common_task(val: int) -> str:
        return f'generated value is {val}'

    val = random_gen()
    if_branch(val) >> [common_task(val), odd_task(val), even_task(val)]

branching()
