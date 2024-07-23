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
def filter_message(message, fields):
    return {field: message[field] for field in fields if field in message}

# Funzione che gestisce il consumo dei messaggi da Kafka per un singolo sensore
def consume_sensor_data(sensor_info):
    broker = sensor_info['broker']
    port = sensor_info['port']
    topic = sensor_info['topic']
    fields = sensor_info['fields_to_monitor']
    
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
        filtered_message = filter_message(message.value['payload'], fields)
        print(f"Messaggio filtrato da {sensor_info['topic']} (ID: {sensor_info['sensor_id']}):", filtered_message)

# Funzione principale per eseguire il monitoraggio on-demand
def monitor_on_demand(sensor_ids, config_file):
    sensor_config = load_sensor_config(config_file)
    threads = []

    for sensor_id in sensor_ids:
        print(sensor_id)
        sensor_info = get_sensor_info(sensor_config, sensor_id)
        print(sensor_info)
        if sensor_info:
            sensor_info['sensor_id'] = sensor_id  # Aggiungiamo il sensor_id alle informazioni del sensore
            thread = threading.Thread(target=consume_sensor_data, args=(sensor_info,))
            thread.daemon = True  # Permette di terminare i thread quando il programma principale termina
            threads.append(thread)
            thread.start()
        else:
            print(f"Warning: Sensor ID {sensor_id} not found in configuration.")
    
    # Manteniamo il programma principale in esecuzione
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Monitoraggio interrotto dall'utente.")

# Esempio di utilizzo
if __name__ == "__main__":
    sensor_ids = ["imu_unit_1", "imu_unit_2", "imu_unit_3"]
    config_file = './VOL200GB/framework/sensors_config.json'
    
    monitor_on_demand(sensor_ids, config_file)
