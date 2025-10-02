from airflow.sdk import asset, Asset, Context

# Question: What is an asset? Similar to ADF Dataset?
# What does it mean to materialize an asset? Anything equivalent in ADF?
@asset(
    schedule='@daily',
    uri='https://randomuser.me/api'
)
def user(self) -> dict[str]:
    '''
        Behind the scenes, an asset is a dag with single activity
        So this is equivalent to a user dag with user activity
    '''
    import requests
    response = requests.get(self.uri)
    return response.json()

# Create multiple assets at once 
@asset.multi(
    schedule = user,
    outlets = [
        Asset(name='user_location_2'),
        Asset(name='user_login_2')
    ]
)
def user_info(user: Asset, context: Context) -> list[dict[str]]:
    user_data = context['ti'].xcom_pull(
        dag_id = user.name,
        task_ids = user.name,
        include_prior_dates = True 
    )
    return [
        user_data['results'][0]['location'],
        user_data['results'][0]['login']
    ]

@asset(
    schedule=user
)
def user_location(user: Asset, context: Context) -> dict[str]:
    user_data = context['ti'].xcom_pull(
        dag_id = user.name,
        task_ids = user.name,
        include_prior_dates = True
    )
    return user_data['results'][0]['location']

@asset(
    schedule=user
)
def user_login(user: Asset, context: Context) -> dict[str]:
    user_data = context['ti'].xcom_pull(
        dag_id = user.name,
        task_ids = user.name,
        include_prior_dates = True
    )
    return user_data['results'][0]['login']
