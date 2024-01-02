
#Importing the required libraries
import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from crypto_etl import dag_crpyto_etl


#setting the default arguments for the DAG in a dictionary named as default_args
default_agrs = {
    'start_date': datetime(2023,10,22),     #the date and time at which the dag is scheduled to trigger
    'retries':1,                            #number of retries if the DAG execution fails
    'retry_delay': timedelta(minutes=1)     #time delay between consecutive retries
}


#defining the DAG
with DAG(
    dag_id= 'crypto_etl',           #defining the dag_id
    default_args= default_agrs      #setting the default_args
    )as dag:

#defining the task to be executed inside the DAG
    api_extract_transform = PythonOperator(             #the operator being used is a Python Operator
        task_id= 'crypto_api_etl',                      #defining the name of the task
        python_callable=dag_crpyto_etl                  #this task will call a python function imported in this code
    )
    