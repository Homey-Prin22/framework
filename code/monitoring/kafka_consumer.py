import json
import threading
from kafka import KafkaConsumer

BROKER = "localhost"
PORT = 9092


def create_kafka_consumer(topic):
    broker = BROKER
    port = PORT
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=f'{broker}:{port}',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def filter_message(message, fields_not_to_monitor):
    #return {field: message[field] for field in fields if field in message}
    return {field: message[field] for field in message if field not in fields_not_to_monitor}

# Funzione che gestisce il consumo dei messaggi da Kafka per un singolo sensore
def consume_sensor_data(sensor_info, stop_event, message_queue):
    topic = sensor_info['topic']
    fields = sensor_info['fields_not_to_monitor']
    
    consumer = create_kafka_consumer(topic)
    
    for message in consumer:
        if stop_event.is_set():
            break
        	
        #print(message.value)
        filtered_message = filter_message(message.value, fields)
        #filtered_message = message
        
        message_queue.put(filtered_message)
