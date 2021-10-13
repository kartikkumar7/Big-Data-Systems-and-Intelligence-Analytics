#Importing Libraries
from google.cloud.storage import bucket
import pandas as pd
from io import BytesIO
import os
from airflow import DAG
from google.cloud import storage
from airflow.operators.python import PythonOperator, task
from airflow.operators.bash import BashOperator
from google.oauth2.credentials import Credentials
import datetime as dt
from pandas.io.parsers import read_csv

key_GCS='' # data path to your key of GCS (.json file) 
project_id='' # your project id
bucket_id='' # your bucket

# Python function to check successful connection with GCS
def connect_GCS():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=key_GCS
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_id)
    print('Successful connection')

# Python function to read details CSV
def GCS_df_det():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=key_GCS 
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_id)

    blob = bucket.blob('/INFO_7245/master_df_details.csv')
    df1 = pd.read_csv("gs://storm_event/INFO_7245/master_df_details.csv")
    print(df1.head())
    df1.to_csv('/mnt/c/DAGS/master_df_details_gcs.csv')


# Python function to read fatalities CSV
def GCS_df_fat():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=key_GCS

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_id)

    blob = bucket.blob('/INFO_7245/master_df_fatalities.csv')
    df2 = pd.read_csv("gs://storm_event/INFO_7245/master_df_fatalities.csv")
    print(df2.head())
    df2.to_csv('/mnt/c/DAGS/master_df_fatalities_gcs.csv')
    
# This Python function is interlinked with clean details, 
# this function modifies values of 'DAMAGE PROPERTY' column
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

# Python function to filter essential columns, 50 states of US 
# and modify value of 'DAMAGE_PROPERTY' column
def clean_details():
    df1=pd.read_csv('/mnt/c/DAGS/master_df_details_gcs.csv')
    print(df1.head())
    df1 = df1[['STATE','EVENT_ID',
       'YEAR', 'EVENT_TYPE', 
       'INJURIES_DIRECT', 'INJURIES_INDIRECT', 'DEATHS_DIRECT',
       'DEATHS_INDIRECT', 'DAMAGE_PROPERTY', 'DAMAGE_CROPS']]

    states = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado",
  "Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois",
  "Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland",
  "Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana",
  "Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
  "North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
  "Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
  "Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]
    df1['DAMAGE_PROPERTY'] = df1['DAMAGE_PROPERTY'].apply(modify_val)
    df1 = df1[~df1['STATE'].isin(states)]
    df1.to_csv('/mnt/c/DAGS/master_df_details_gcs_cleaned.csv')

# Python function to filter essential columns
def clean_fatalities():
    df2= read_csv('/mnt/c/DAGS/master_df_fatalities_gcs.csv')
    df2 = df2[['EVENT_ID','FATALITY_AGE','FATALITY_SEX','FATALITY_LOCATION']]
    df2.to_csv('/mnt/c/DAGS/master_df_fatalities_gcs_cleaned.csv')

# Python function to merge cleaned dataframes of details and fatalities
def merge_csv():
    
    df1=read_csv('/mnt/c/DAGS/master_df_details_gcs_cleaned.csv')
    df2=read_csv('/mnt/c/DAGS/master_df_fatalities_gcs_cleaned.csv')
    df3 = df1.merge(df2, how='inner', on='EVENT_ID')
    df3.to_csv('/mnt/c/DAGS/master_df_merged.csv')

# Python function to upload merged CSV to GCS
def csv_to_GCS():
    df_final= read_csv('/mnt/c/DAGS/master_df_merged.csv')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=key_GCS
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_id)
    df_final.to_csv('gs://storm_event/INFO_7245/master_df_merged.csv')
    print('Successful upload')


with DAG(
    "gcs_to_df",
    start_date=dt.datetime(2021, 10, 10),
    schedule_interval='@once') as dag:

    t1 = PythonOperator(
        task_id="connect_to_gcs",
        python_callable=connect_GCS
    )

    t2 = PythonOperator(
        task_id="connect_to_gcs_read_details",
        python_callable=GCS_df_det
    )
    t3 =PythonOperator(
        task_id="connect_to_gcs_read_fatalities",
        python_callable=GCS_df_fat
    )
    t4 = PythonOperator(
        task_id="clean_details_df",
        python_callable=clean_details,
    )
    t5=PythonOperator(
        task_id="clean_fatalities_df",
        python_callable=clean_fatalities,
    )
    t6 = PythonOperator(
        task_id="merge_details_fatalities",
        python_callable=merge_csv
    )
    t7 = PythonOperator(
        task_id='upload_to_GCS',
        python_callable=csv_to_GCS
    )
    
    # upstreaming the tasks
    t1 >>[t2,t3]
    t2 >> t4
    t3 >> t5
    [t5,t4]>>t6>>t7 

    