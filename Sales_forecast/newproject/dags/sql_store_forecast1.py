# -*- coding: utf-8 -*-
"""
Created on Thu May 11 19:37:17 2023

@author: Rakshitha Krishnan
"""

import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from model_forecast1 import forecasted_sales

def store_forecast():
    fcast = forecasted_sales()
    conn = BaseHook.get_connection('sqlconnect1')
    engine = create_engine(f'mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    fcast.to_sql('cook_time_predicted', engine, index=False)
    ef = pd.read_sql_query('SELECT * FROM  storeforecast.cook_time_predicted',engine)
    return ef