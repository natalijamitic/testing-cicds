source .venv/bin/activate
export AIRFLOW_HOME=${PWD}/airflow
airflow users create --username admin --firstname John --lastname Doe --role Admin --email john@doe.com