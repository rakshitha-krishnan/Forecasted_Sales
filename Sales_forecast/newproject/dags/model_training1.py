# -*- coding: utf-8 -*-
"""
Created on Thu May 11 19:28:56 2023

@author: Rakshitha Krishnan
"""

import pandas as pd
import logging
from pmdarima.arima import auto_arima
import warnings
from pmdarima.arima import ADFTest
from data_connection1 import analyze_data


def stationarity_test():
    new_sales_data = analyze_data()
    adf_test = ADFTest(alpha=0.05)
    result = adf_test.should_diff(new_sales_data['Column1.ItemCookTime'])
    logging.info(f'###From the values we can see if model stationary or not:{result}')
    
def model_training():
    new_sales_data = analyze_data()
    train = new_sales_data[:160]
    test = new_sales_data[-40:]
    ind_features = ['Column1.CourseName', 'Column1.CourseNumber', 'Column1.CoursePreparationTime', 'Column1.CourseStartTimeLocal', 'Column1.CourseType', 'Column1.ItemCookStartTime', 'Column1.ItemQuantity']
    warnings.filterwarnings('ignore')
    model=auto_arima(y=train['Column1.ItemCookTime'], exogenous=train[ind_features], trace=True)
    logging.info('f### After performing stepwise search the best model is :{model.summary()}')
    model.fit(train['Column1.ItemCookTime'])
    return model