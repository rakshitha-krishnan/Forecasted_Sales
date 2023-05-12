# -*- coding: utf-8 -*-
"""
Created on Thu May 11 19:34:09 2023

@author: Rakshitha Krishnan
"""

import pandas as pd
import logging
from pmdarima.arima import auto_arima
import warnings
from statsmodels.tsa.statespace.sarimax import SARIMAX
from model_training1 import model_training
from data_connection1 import analyze_data

def forecasted_sales():
    model = model_training()
    new_sales_data = analyze_data()
    test = new_sales_data[-40:]
    ind_features = ['Column1.CourseName', 'Column1.CourseNumber', 'Column1.CoursePreparationTime', 'Column1.CourseStartTimeLocal', 'Column1.CourseType', 'Column1.ItemCookStartTime', 'Column1.ItemQuantity']
    forecast = model.predict(n_periods=len(test), exogenous=test[ind_features])
    logging.info('f### The predictions on the testing set are: {forecast}')
    forecast_model = SARIMAX(new_sales_data["Column1.ItemCookTime"], order=(1,0,0))
    result = forecast_model.fit()
    fcast = result.predict(len(new_sales_data), len(new_sales_data)+30, type='levels').rename('Column1.ItemCookTime')
    return fcast

