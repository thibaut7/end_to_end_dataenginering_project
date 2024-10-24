from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
""" default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2)
}
print('thi') """

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
    from kafka import KafkaProducer

    res = get_data()
    res = format_data(res)
    #return json.dumps(res, indent=3)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(res).encode('utf-8'))

stream_data()