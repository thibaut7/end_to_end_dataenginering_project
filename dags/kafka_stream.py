from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json


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

    res = get_data()
    #return json.dumps(res, indent=3)

    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    try:
        producer.send('users_created', json.dumps(res).encode('utf-8'))
        producer.flush()  # Ensure all messages are sent
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

print(stream_data())