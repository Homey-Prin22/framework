import json
import threading
from kafka import KafkaConsumer

BROKER = "localhost"
PORT = 9092

def kafka_consumer(topic):
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