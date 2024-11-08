from datetime import datetime
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json

default_args ={
    'owner': 'thibaut7',
    'start_date': datetime(2024, 11, 8, 18, 3, 00)
}

def get_data():
    url = 'https://randomuser.me/api/'
    response = requests.get(url)
    data = {}
    if response.status_code == 200:
        #data = json.loads(response.json())
        res = response.json()
        res = res['results'][0]
        data['firstname'] = res['name']['first']
        data['lastname'] = res ['name']['last']
        data['address'] = str(res['location']['street']['number']) + ' ' + str(res['location']['street']['name']) + ', ' + str( res['location']['city'])+ ', ' + str(res['location']['state']) + ', '  +  str(res['location']['country'])
        
        data['postcode'] = res['location']['postcode']
        data['email'] = res['email']
        data['username'] = res['login']['username']
        data['password'] = res['login']['password']
        data['birth_date'] = res['dob']['date']
        data['registered_data'] = res['registered']['date']
        data['phone'] = res['phone']
        data['picture'] = res['picture']['medium']
    else:
        print(f"Erreur: {response.status_code}")
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()
    
    while True:
        if time.time() > current_time + 60:
            break
        try:
            res = get_data()
            producer.send('users_created', json.dumps(res).encode('utf-8'))
            producer.flush()  # Ensure all messages are sent
        except Exception as e:
            logging.error('An error occured: {e}')
            continue

with DAG('user_automation',
        default_args=default_args,
        schedule='@hourly',
        catchup=False) as dag:
        
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
        )


