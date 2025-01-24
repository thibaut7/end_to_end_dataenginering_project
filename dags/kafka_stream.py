from datetime import datetime, timedelta
import uuid
import requests
import json
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

default_args ={
    'owner': 'admin',
    'start_date': datetime(2025, 1, 23)  # Start immediately
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
    from confluent_kafka import Producer
    import time
    import logging
    from time import sleep

    # Producer configuration with Docker network settings
    config = {
        'bootstrap.servers': 'kafka1:29092,kafka2:29093',  # Docker internal network addresses
        'client.id': 'python-producer',
        'retries': 5,
        'retry.backoff.ms': 1000
    }

    # Create Producer instance with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = Producer(config)
            break
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Connection attempt {attempt + 1} failed, retrying...")
            sleep(2)
    
    try:
        while True:
            # Get data from API
            data = get_data()
            
            # Produce message
            producer.produce(
                topic='user_data',
                key=str(time.time()),
                value=json.dumps(data),
                callback=delivery_report
            )
            
            # Flush producer queue
            producer.poll(0)
            time.sleep(1)
            
    except KeyboardInterrupt:
        print('Interrupted by user')
    finally:
        producer.flush(timeout=5)
        
with DAG('user_automation',
        default_args=default_args,
        schedule_interval=timedelta(minutes=2),  # Run every 2 minutes
        catchup=False) as dag:
        
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
        )    
# if __name__ == '__main__':
#     stream_data()

