from airflow.sdk import dag, task, task_group

@dag 
def group_dag():

    @task
    def t1():
        print('t1')
        return 1

    @task_group(
        default_args={
            'retries':2
        }
    )
    def grp1(val: int):

        @task
        def t2(my_val: int):
            # Make sure to name the variables differently in a group and its task
            # Otherwise you will get errors
            print('t2')
            print(my_val + 2)

        @task_group(
            default_args={
                'retries':3
            }
        )
        def grp2():

            @task
            def t3():
                print('t3')

            @task
            def t4():
                print('t4')

            t3() >> t4()

        t2(val) >> grp2()

    val = t1() 
    grp1(val)

group_dag()
