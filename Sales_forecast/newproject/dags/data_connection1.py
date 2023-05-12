# -*- coding: utf-8 -*-
"""
Created on Thu May 11 19:16:51 2023

@author: Rakshitha Krishnan
"""


import time
from datetime import datetime
from airflow.models.dag import DAG
import logging
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder


def get_src_table():
   conn = BaseHook.get_connection('sql_connect1')
   engine = create_engine(f'mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
   df = pd.read_sql_query('SELECT * FROM  storeforecast.transformed_data',engine)
   return df
   
def analyze_data(df:pd.DataFrame):
    sales_data = df
    determine_if_null =sales_data.isnull().value_counts()
    logging.info(f'### How many null values are there: {determine_if_null}')
    logging.info(f'### What are the column datatypes : {sales_data.dtypes}')
    desired_columns = ['Column1.CourseName', 'Column1.CourseNumber', 'Column1.CoursePreparationTime', 'Column1.CourseStartTimeLocal', 'Column1.CourseType','Column1.ItemCookStartTime','Column1.ItemCookTime','Column1.ItemQuantity']
    label_encoder = LabelEncoder()
    new_sales_data = sales_data[desired_columns].copy()
    new_sales_data['Column1.CourseName'] = label_encoder.fit_transform(new_sales_data['Column1.CourseName'])
    new_sales_data['Column1.CourseType'] = label_encoder.fit_transform(new_sales_data['Column1.CourseType'])
    new_sales_data['Column1.CourseStartTimeLocal']=pd.to_datetime(new_sales_data['Column1.CourseStartTimeLocal'])
    return new_sales_data