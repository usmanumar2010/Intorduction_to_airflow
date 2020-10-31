# load the dependencies
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime

# default_args are the default arguments applied to the Dag's tasks
DAG_DEFAULT_ARGS = {
	'owner': 'airflow',
	'start_date':datetime(2020,10,31)
}

with DAG(dag_id="twitter_dag",schedule_interval="@daily",default_args=DAG_DEFAULT_ARGS) as dag:
	waiting_for_tweets=FileSensor(task_id="waiting_for_tweets",fs_conn_id="fs_tweet"	,filepath="data.csv",poke_interval=5)

