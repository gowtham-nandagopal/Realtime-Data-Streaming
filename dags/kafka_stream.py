from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json
import requests
import logging
import time

default_args = {
    'owner': 'gowtham',
    'start_date': datetime(2024, 12, 25, 10, 00)
}

def get_data():
    # Read the data from API source and convert to json format
    results = requests.get("https://randomuser.me/api/")
    respond = results.json()
    respond = respond['results'][0]
    return respond

def format_data(result):
    # From the json response pull out the required information
    data = {}
    location = result['location']
    data['first_name'] = result['name']['first']
    data['last_name'] = result['name']['last']
    data['gender'] = result['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']} " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = result['email']
    data['username'] = result['login']['username']
    data['dob'] = result['dob']['date']
    data['registered_data'] = result['registered']['date']
    data['phone'] = result['phone']
    data['picture'] = result['picture']['medium']

    return data

def stream_data():
    result = get_data()
    json_formatted_result = format_data(result)
    kafka_producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()

    while True:
        if time.time() > current_time + 60: #1 minute
            break
        try:
            result = get_data()
            json_formatted_result = format_data(result)
            kafka_producer.send('users_created', json.dumps(json_formatted_result).encode('utf-8'))

        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue


with DAG(
        dag_id = 'user_automation',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        stream_task = PythonOperator(
            task_id = 'stream_data_from_api',
            python_callable=stream_data
        )