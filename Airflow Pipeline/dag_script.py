import requests
import json

from airflow import DAG
from datetime import datetime
from datetime import date,timedelta

from airflow.operators.python import PythonOperator

import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
import pandas as pd
import numpy as np
import logging
import os
import json

# If you are going to run this code please change the path variable
# to a path that works for you. Because the default path for my airflow
# saved the csv files to a folder in ubuntu and im using windows so i couldnt
# see the files so i changed it to a folder that i have my dags
# in on windows :D
path = '/e/dags'
now = datetime.now()
dateTime= now.strftime("%d-%m-%Y_%H:%M:%S")

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date': datetime(2021,12,24),
	'retries':1,
	'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'Milestone-3',
    default_args=default_args,
    description = 'Data Pipeline',
	start_date=datetime(2021, 12, 21),
    schedule_interval = '@once',
)


def read_olympic(**kwargs):
    # This line changes the current directory of the python file
    # i had problems with ubuntu on windows and folders so this fixed it
    os.chdir(path)
    # read csv files and conver to json so that it can be passed between methods
    df_olympic = pd.read_csv('athlete_events.csv').to_json()
    return df_olympic

def read_noc(**kwargs):
	os.chdir(path)
	df_noc = pd.read_csv('noc_regions.csv').to_json()
	return df_noc

def read_medals(**kwargs):
	os.chdir(path)
	df_medals = pd.read_csv('Medals.csv',encoding = "ISO-8859-1").to_json()
	return df_medals

def clean_data(**context):
	os.chdir(path)
	# Get data from read_olympic
	# convert from json to dataframe

	df_olympic = pd.DataFrame(json.loads(context['task_instance'].xcom_pull(task_ids='Read_Olympic')))
	# Start cleaning process
	missing_values_count_athletes = df_olympic.isnull().sum()
	# Multivariant data imputation for the weight and height using the median and mean of the values with the same sex and sport
	df_olympic['Weight'].fillna(df_olympic.groupby(['Sex','Sport'])['Weight'].transform('median'),inplace=True)
	df_olympic['Height'].fillna(df_olympic.groupby(['Sex','Sport'])['Height'].transform('mean'),inplace=True)
	# Some sports dont have any entries for the weight and height so they are still Nan so we will fill them with the sex median and mean only
	df_olympic['Weight'].fillna(df_olympic.groupby(['Sex'])['Weight'].transform('median'),inplace=True)
	df_olympic['Height'].fillna(df_olympic.groupby(['Sex'])['Height'].transform('mean'),inplace=True)
	# Save cleaned data as csv
	return df_olympic.to_json()

def integrate_data(**context):
	os.chdir(path)
	# get the cleaned data from the clean_data method
	df_olympic = pd.DataFrame(json.loads(context['task_instance'].xcom_pull(task_ids='Clean_Data')))
	# get noc and medals from read_data
	df_regions = pd.DataFrame(json.loads(context['task_instance'].xcom_pull(task_ids='Read_NOC')))
	df_medals = pd.DataFrame(json.loads(context['task_instance'].xcom_pull(task_ids='Read_Medals')))
	# start integration process to create no integrated dataset
	req=df_olympic[['Team','NOC','Year','Medal']][~df_olympic['Medal']. isna()]
	df_olympic_noc=pd.merge(req, df_regions).drop(['notes'],axis=1)
	#os.makedirs('Olympic')
	df_olympic_noc.to_csv('Olympic/noc_dataset.csv')
	# add year column to df_medals and remove unwanted columns
	df_medals=df_medals.drop(['Rank','Rank by Total'],axis=1)
	df_medals['Year']=2021
	df_medals=df_medals[['Team/NOC','Gold','Silver','Bronze','Total']]

	# Create the count for each column
	gold = df_olympic_noc.loc[df_olympic_noc['Medal']=='Gold']
	gold = pd.Series(gold.groupby(['NOC','Year'])['Medal'].size(), dtype=np.int64,name='Gold')
	silver = df_olympic_noc.loc[df_olympic_noc['Medal']=='Silver']
	silver = pd.Series(silver.groupby(['NOC','Year'])['Medal'].size(), dtype=np.int64,name='Silver')
	bronze = df_olympic_noc.loc[df_olympic_noc['Medal']=='Bronze']
	bronze = pd.Series(bronze.groupby(['NOC','Year'])['Medal'].size(), dtype=np.int64,name='Bronze')
	total= pd.Series(df_olympic_noc.groupby(['NOC','Year']).size(), dtype=np.int64, name='Total')
	# Merge the columns
	new_olympic =pd.merge(df_olympic_noc,gold,how='left',left_on=['NOC','Year'], right_on = ['NOC','Year'])
	new_olympic =pd.merge(new_olympic,silver,how='left',left_on=['NOC','Year'], right_on = ['NOC','Year'])
	new_olympic =pd.merge(new_olympic,bronze,how='left',left_on=['NOC','Year'], right_on = ['NOC','Year'])
	new_olympic =pd.merge(new_olympic,total,how='left',left_on=['NOC','Year'], right_on = ['NOC','Year'])
	new_olympic = new_olympic.fillna(0)
	new_olympic =new_olympic.astype({"Gold":'int',"Silver":'int', "Bronze":'int',  "Total":'int'})
	new_olympic = new_olympic.drop(['Medal','region','NOC'],axis=1)
	new_olympic = new_olympic.rename(columns={"Team": "Team/NOC"})
	# create new integrated dataset
	df_teamMedals=pd.concat([new_olympic,df_medals])
	return df_teamMedals.to_json()


def feature_data(**context):
	# get the cleaned data from the clean_data method
	df_olympic = pd.DataFrame(json.loads(context['task_instance'].xcom_pull(task_ids='Clean_Data')))
	# Add bmi feature
	df_olympic['BMI']=(df_olympic['Weight'])/(df_olympic['Height']**2/100)

	# Add all time total
	df_medals=df_olympic[df_olympic['Medal'].isnull()==False]
	df_athleteTotal = pd.Series(df_medals.groupby('Name')['Name'].count().sort_values(ascending=False),name='All Time Total').fillna(0)
	df_olympic=pd.merge(df_olympic,df_athleteTotal,how='left',left_on=['Name'], right_on = ['Name'])
	df_olympic['All Time Total']=df_olympic['All Time Total'].fillna(0)
	df_olympic=df_olympic.astype({"All Time Total":'int'});
	return df_olympic.to_json()

def load_data(**context):
	os.chdir(path)
	# save from each method to csv
	cleaned_data     = pd.DataFrame(json.loads(context['task_instance'].xcom_pull(task_ids='Clean_Data')))
	integrated_data = pd.DataFrame(json.loads(context['task_instance'].xcom_pull(task_ids='Integrate_Data')))
	featured_data   = pd.DataFrame(json.loads(context['task_instance'].xcom_pull(task_ids='Feature_data')))
	# save them
	cleaned_data.to_csv('Olympic'+'/cleaned_dataset.csv')
	integrated_data.to_csv('Olympic'+'/integrated_dataset.csv')
	featured_data.to_csv('Olympic'+'/featured_dataset.csv')





read_olympic = PythonOperator(
    task_id = 'Read_Olympic',
    provide_context = True,
    python_callable=read_olympic,
    dag=dag,
)
read_noc= PythonOperator(
    task_id = 'Read_NOC',
    provide_context = True,
    python_callable=read_noc,
    dag=dag,
)
read_medals= PythonOperator(
    task_id = 'Read_Medals',
    provide_context = True,
    python_callable=read_medals,
    dag=dag,
)
clean = PythonOperator(
    task_id = 'Clean_Data',
    provide_context = True,
    python_callable=clean_data,
    dag=dag,
)
integrate = PythonOperator(
	task_id = 'Integrate_Data',
    provide_context = True,
    python_callable=integrate_data,
    dag=dag,
)


feature = PythonOperator(
	task_id = 'Feature_data',
    provide_context = True,
    python_callable=feature_data,
    dag=dag,
)
load = PythonOperator(
	task_id = 'Save_Data',
    provide_context = True,
    python_callable=load_data,
    dag=dag,
)


# here we decide which tasks are dependant on others
read_olympic>>clean>>integrate
read_noc>>integrate
read_medals>>integrate
clean>>feature
integrate>>load
feature>>load
