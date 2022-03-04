import os
import json
import csv
import logging
import requests
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from airflow import DAG
import time
from datetime import datetime
from datetime import date,timedelta
from dotenv import dotenv_values
from nltk.sentiment import SentimentIntensityAnalyzer
from airflow.operators.python_operator import PythonOperator

# Need to have enviroment variables file to run

search_url = "https://api.twitter.com/2/tweets/search/recent"
sia = SentimentIntensityAnalyzer()
# Spain underperformed and  won 17 medals only while USA won 113


bearer_token = 'AAAAAAAAAAAAAAAAAAAAAJrfXQEAAAAABI1IZ69Q1NBp3i7DBdxmD9sJm0Q%3DgYtIkBbjyrIjnpQY8ho7cdKvNnCV7pHOZ13iaRqUAhkAzqDs7w'

usa_tweets=0
spain_tweets = 0
path = '/e/dags'
now = datetime.now()
dateTime= now.strftime("%d-%m-%Y")



default_args = {
    'owner':'airflow',
    'depends_on_past':True,
    'start_date': datetime(2021,12,28),
	'retries':1,
	'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'Twitter_API',
    default_args=default_args,
    description = 'Date Pipeline',
	start_date=datetime(2021, 12, 28),
    schedule_interval = '@daily',
	params={"my_param": "{{ execution_date }}"}
)


def bearer_oauth(r):
	r.headers["Authorization"] =f"Bearer {bearer_token}"
	logging.info('Helloo',bearer_token)
	r.headers["User-Agent"] ="v2RecentSearchPython"
	return r

def connect_to_endpoint(url, params):
	response = requests.get(url,auth=bearer_oauth, params=params)
	if response.status_code != 200:
			raise Exception(response.status_code, response.text)
	return response.json()

def average_sentiment(tweets):
    score=0
    for i in tweets:
        score+=sia.polarity_scores(i)['compound']
    return score/len(tweets)

def list_tweets(s):
	list1=[]
	with open(s,encoding='utf-8') as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
		for row in csv_reader:
			list1.append(row[0])
	return list1


def fetch_usa(execution_date,**kwargs):
	os.chdir(path)
	# Fetch the tweets from the twitter api
	usa_query_params = {
	'query': '(usa) (Tokyo Olympics OR #TokyoOlympics2020) -is:retweet lang:en',
	'tweet.fields': 'author_id',
	'max_results':20,
	'end_time':execution_date}
	usa_tweets = connect_to_endpoint(search_url, usa_query_params)['data']
	# If file doesnt exist create it
	if(not os.path.isfile('Twitter_Data/usa_tweets.csv')):
		open('Twitter_Data/usa_tweets.csv','x')
	# append tweets to the csv
	with open('Twitter_Data/usa_tweets.csv', 'a', newline='',encoding='utf-8') as f:
		writer = csv.writer(f)
		for i in usa_tweets:
			writer.writerow([i['text']])

def fetch_spain(execution_date,**context):
	os.chdir(path)
	spain_query_params = {
	'query': '(spain) (Tokyo Olympics OR #TokyoOlympics2020 OR Olympics) -is:retweet',
	'tweet.fields': 'author_id',
	'max_results':20,
	'end_time':execution_date}
	spain_tweets = connect_to_endpoint(search_url, spain_query_params)['data']
	if(not os.path.isfile('Twitter_Data/spain_tweets.csv')):
		open('Twitter_Data/spain_tweets.csv','x')
	with open('Twitter_Data/spain_tweets.csv', 'a', newline='',encoding='utf-8') as f:
		writer = csv.writer(f)
		for i in spain_tweets:
			writer.writerow([i['text']])

def sentiment_analysis(execution_date,**context):

	# Get the tweets from the csv file
	os.chdir(path)
	usa_tweets=list_tweets('Twitter_Data/usa_tweets.csv')
	spain_tweets=list_tweets('Twitter_Data/spain_tweets.csv')
	# get average sentiment of tweets so far
	usa_average=average_sentiment(usa_tweets)
	spain_average=average_sentiment(spain_tweets)
	# Create csv file if it doesnt exist and add headers
	if(not os.path.isfile('Twitter_Data/sentiment_score.csv')):
		with open('Twitter_Data/sentiment_score.csv', 'w', newline='') as file:
			writer = csv.writer(file)
			writer.writerow(["Date","USA","Spain"])
	# add todays average score to the list
	with open('Twitter_Data/sentiment_score.csv', 'a', newline='') as f:
		writer = csv.writer(f)
		writer.writerow([str(execution_date.date()),usa_average,spain_average])




get_usaTweets = PythonOperator(
    task_id = 'Fetch_USA',
    provide_context = True,
    python_callable=fetch_usa,
    dag=dag,
)
get_spainTweets = PythonOperator(
    task_id = 'Fetch_Spain',
    provide_context = True,
    python_callable=fetch_spain,
    dag=dag,
)
analysis = PythonOperator(
    task_id = 'Sentiment_Analysis',
    provide_context = True,
    python_callable=sentiment_analysis,
    dag=dag,
)


get_usaTweets>>analysis
get_spainTweets>>analysis
