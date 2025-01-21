from datetime import datetime
import uuid
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

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def stream_data():
    import json
    from confluent_kafka import Producer
    import time
    import logging

    # Producer configuration
    config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'python-producer'
    }

    # Create Producer instance
    producer = Producer(config)
    
    # Run until interrupted
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
            
            # Wait for 1 second
            time.sleep(1)
            
    except KeyboardInterrupt:
        print('Interrupted by user')
    finally:
        # Flush any remaining messages
        producer.flush()

if __name__ == '__main__':
    stream_data()

