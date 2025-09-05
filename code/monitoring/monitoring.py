import json
import threading
from kafka import KafkaConsumer

# Funzione per leggere la configurazione dei sensori da un file JSON
def load_sensor_config(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)

# Funzione per ottenere le informazioni di un sensore dato il sensor_id
def get_sensor_info(sensor_config, sensor_id):
    sensor_id = str(sensor_id)
    return sensor_config.get(sensor_id, None)

# Funzione che filtra i messaggi in base ai campi specificati
def filter_message(message, fields_to_exclude):
    #return {field: message[field] for field not in fields_to_exclude if field in message}
    return {field: message[field] for field in message if field not in fields_to_exclude}

# Funzione che gestisce il consumo dei messaggi da Kafka per un singolo sensore
def consume_sensor_data(sensor_info):
    broker = sensor_info['broker']
    port = sensor_info['port']
    topic = sensor_info['topic']
    fields_to_exclude = sensor_info['fields_not_to_monitor']
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=f'{broker}:{port}',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        #group_id='sensor-monitor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for message in consumer:
        print(message.value)
        filtered_message = filter_message(message.value['payload'], fields_to_exclude)
        print(f"Messaggio filtrato da {sensor_info['topic']} (ID: {sensor_info['source_id']}):", filtered_message)

