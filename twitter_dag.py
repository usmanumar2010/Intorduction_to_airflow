# load the dependencies
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime
import fetching_tweet

# default_args are the default arguments applied to the Dag's tasks
DAG_DEFAULT_ARGS = {
	'owner': 'airflow',
	'start_date':datetime(2020,10,31)
}

with DAG(dag_id="twitter_dag",schedule_interval="@daily",default_args=DAG_DEFAULT_ARGS) as dag:
	waiting_for_tweets=FileSensor(task_id="waiting_for_tweets",fs_conn_id="fs_tweet",filepath="data.csv",poke_interval=5)

	fetching_tweet_task = PythonOperator(task_id="fetching_tweet_task", python_callable=fetching_tweet.main)

	cleaning_tweets = PythonOperator(task_id="cleaning_tweets", python_callable=cleaning_tweet.main)
