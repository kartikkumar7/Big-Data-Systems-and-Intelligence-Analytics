#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  6 12:20:07 2021

@author: kartikkumar
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from textwrap import dedent
import pandas as pd
import numpy as np



def modify_val(val):
    
    if 'K' in str(val):
        try:
            string = str(val[:-1])
            new_val = float(string) * 1000
            val = new_val
        except ValueError:
            pass
    elif 'M' in str(val):
        try:
            string = str(val[:-1])
            new_val = float(string) * 1000000
            val = new_val
        except ValueError:
            pass
    return val

def read_csv(path):
    df = pd.read_csv(filepath_or_buffer=path)
    #print(df.columns)
    df = df[['STATE','EVENT_ID',
       'YEAR', 'EVENT_TYPE', 
       'INJURIES_DIRECT', 'INJURIES_INDIRECT', 'DEATHS_DIRECT',
       'DEATHS_INDIRECT', 'DAMAGE_PROPERTY', 'DAMAGE_CROPS', 'SOURCE',
       'MAGNITUDE', 'MAGNITUDE_TYPE', 'FLOOD_CAUSE', 'CATEGORY']]
    
    #for val in df['DAMAGE_PROPERTY'].values:
    states = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado",
  "Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois",
  "Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland",
  "Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana",
  "Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
  "North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
  "Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
  "Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]

        
    df['DAMAGE_PROPERTY'] = df['DAMAGE_PROPERTY'].apply(modify_val)
    df = df[~df['STATE'].isin(states)]
    df.to_csv('/Users/kartikkumar/Documents/NU/7245/Assignment1/processed_details.csv')
    
    
    

    
    
def print_success():
    print("Tasks completed! Check file!!")

default_args = {
    'owner': 'Kartik Kumar',
    'start_date': days_ago(1)
    }
# Defining the DAG using Context Manager
with DAG(
        'read_and_print_head',
        default_args=default_args,
        schedule_interval=None,
        ) as dag:
        
        
        
        
         t1 = PythonOperator(
                 task_id = 'read_file',
                 python_callable = read_csv,
                 op_kwargs={'path': '/Users/kartikkumar/Documents/NU/7245/Assignment1/master_df_details.csv'}
                 )
         t2 = PythonOperator(
                 task_id = 'success',
                 python_callable = print_success
                 )
         t1 >> t2 # Defining the task dependencies
         
 
         
 
      
