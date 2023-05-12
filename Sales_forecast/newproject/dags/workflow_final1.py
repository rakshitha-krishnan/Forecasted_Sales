# -*- coding: utf-8 -*-
"""
Created on Thu May 11 19:15:35 2023

@author: Rakshitha Krishnan
"""


from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from data_connection1 import get_src_table
from data_connection1 import analyze_data
from model_training1 import stationarity_test
from model_training1 import model_training
from model_forecast1 import forecasted_sales
from sql_store_forecast1 import store_forecast

default_args = {"owner":"airflow", "start_date":datetime(2023,11,5)}
with DAG (dag_id="workflow_final1",default_args=default_args,schedule_interval='@daily') as dag:
    
    get_src_table = PythonOperator(
        task_id = 'get_src_table',
        python_callable = get_src_table
        )
    
    analyze_data = PythonOperator(
        task_id = 'analyze_data',
        python_callable = analyze_data
        )
    
    stationarity_test = PythonOperator(
        task_id='stationarity_test',
        python_callable=stationarity_test,
    )
    
    model_training = PythonOperator(
        task_id='model_training',
        python_callable=model_training,
        dag=dag,
    )

    forecasted_sales = PythonOperator(
        task_id='forecasted_sales',
        python_callable=forecasted_sales,
        dag=dag,
    )

    store_forecast = PythonOperator(
        task_id='store_forecast',
        python_callable=store_forecast,
        dag=dag,
    )
    get_src_table >> analyze_data >> stationarity_test >> model_training >> forecasted_sales >> store_forecast
