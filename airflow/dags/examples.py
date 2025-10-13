from datetime import datetime

import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "natalija",
    "retries": 1,
    "start_date": datetime(2025, 10, 4),
}


def print_start():
    print("Starting Airflow DAG")


def print_date():
    print(f"Current date and time: {datetime.now()}")


with DAG(
    "simple_dag",
    start_date=datetime(2024, 9, 18),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag1:
    start_task = PythonOperator(task_id="print_start", python_callable=print_start)
    date_task = PythonOperator(task_id="print_date", python_callable=print_date)

    start_task >> date_task


def print_hello():
    print("Hello from Airflow")


with DAG(
    "operators_dag",
    start_date=datetime(2024, 9, 18),
    schedule_interval="*/5 * * * *",
    default_args=default_args,
    catchup=False,
) as dag2:
    bash_task = BashOperator(
        task_id="bash_task", bash_command='echo "Running Bash task"'
    )

    python_task = PythonOperator(task_id="python_task", python_callable=print_hello)

    bash_task >> python_task


# Task to push data into XCom
def push_data_to_xcom(**context):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context["ti"].xcom_push(
        key="current_time", value=f"{current_time} from python function"
    )


# DAG definition
with DAG(
    "xcom_example_dag",
    start_date=datetime(2024, 9, 18),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag3:

    # PythonOperator to push data
    push_task = PythonOperator(
        task_id="push_data", python_callable=push_data_to_xcom, provide_context=True
    )

    # BashOperator to pull data and echo it
    pull_task = BashOperator(
        task_id="pull_data",
        bash_command='echo "The current time is {{ ti.xcom_pull(task_ids="push_data", key="current_time") }}"',
    )

    push_task >> pull_task


# Fetch weather data
def fetch_weather_data(**context):
    api_key = "FR8TL7DLPK3QRT2NKBJ5G7WNU"
    city = "London"
    response = requests.get(
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}?unitGroup=metric&include=current&key={api_key}&contentType=json"
    )
    data = response.json()
    temp = data["currentConditions"]["temp"]
    context["ti"].xcom_push(key="temperature", value=temp)


# Process data
def process_data(**context):
    temperature = context["ti"].xcom_pull(
        task_ids="fetch_weather_data", key="temperature"
    )
    temp_k = temperature + 273.15
    context["ti"].xcom_push(key="temp_K", value=temp_k)


def save_to_file(**context):
    temp_k = context["ti"].xcom_pull(task_ids="process_data", key="temp_K")
    with open("/Users/klokar/Desktop/testing-cicds/tmp/weather_data.txt", "w") as f:
        f.write(f"Current temperature: {temp_k} °K")


def trigger_github_workflow(**context):
    temperature = context["ti"].xcom_pull(
        task_ids="fetch_weather_data", key="temperature"
    )

    github_token = "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"
    repo_owner = "natalijamitic"
    repo_name = "testing-cicds"
    workflow_file = "print_temperature.yml"
    workflow_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_file}/dispatches"

    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    payload = {"ref": "main", "inputs": {"temperature": str(temperature)}}

    response = requests.post(workflow_url, json=payload, headers=headers)
    if response.status_code == 204:
        print("GitHub workflow triggered successfully")
    else:
        print(f"Failed to trigger GitHub workflow: {response.status_code}")


# DAG definition
with DAG(
    "weather_pipeline",
    start_date=datetime(2024, 9, 19),
    schedule_interval=None,
    catchup=False,
) as dag4:
    fetch_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id="process_data", python_callable=process_data, provide_context=True
    )

    save_task = PythonOperator(
        task_id="save_data", python_callable=save_to_file, provide_context=True
    )

    github_task = PythonOperator(
        task_id="github_task",
        python_callable=trigger_github_workflow,
        provide_context=True,
    )

    fetch_task >> process_task >> save_task
    fetch_task >> github_task
