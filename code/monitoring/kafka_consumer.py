import json
import threading
from kafka import KafkaConsumer

BROKER = "localhost"
PORT = 9092

def load_sensor_config(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)

#def get_sensor_info(sensor_config, sensor_id):
#    sensor_id = str(sensor_id)
#    return sensor_config.get(sensor_id, None)

#def get_sensors_in_site(sensor_config, site):
#    sensors = [sensor for sensor in sensor_config.values() if sensor['site'] == site]
#    return sensors

#def filter_message(message, fields):
#    return {field: message[field] for field in fields if field in message}

def create_kafka_consumer(sensor_info):
    broker = BROKER
    port = PORT
    print(sensor_info)
    topic = sensor_info['topic']
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=f'{broker}:{port}',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def consume_sensor_data(sensor_info, stop_event, message_queue):
    consumer = create_kafka_consumer(sensor_info)
    fields = sensor_info['fields_to_monitor']
    
    for message in consumer:
        if stop_event.is_set():
            break

        #if site == 'lab':
        message_value = message.value['payload']
        #filtered_message = filter_message(message_value, fields)
        #message_queue.append(filtered_message)
        message_queue.append(message_value)
        #else:
        #    if message.value['room'] == site:
        #        filtered_message = filter_message(message.value, fields)
        #        message_queue.append(filtered_message)
