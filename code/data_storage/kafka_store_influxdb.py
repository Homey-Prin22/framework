import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions
import threading
from influxdb_client.client.write_api import SYNCHRONOUS
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


# Funzione per caricare la configurazione dei sensori da file JSON
def load_sensor_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)

# Funzione per caricare la configurazione del database da file JSON
def load_db_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)

# Funzione per creare un client InfluxDB
def create_influxdb_client(db_config):
    try:
        client = InfluxDBClient(
            url=db_config['address'],
            token=db_config['token'],
            org=db_config['org']
        )
        print("Connected to InfluxDB successfully.")
        return client
    except Exception as e:
        print(f"Failed to connect to InfluxDB: {e}")
        return None


# Funzione per controllare che il bucket esista, altrimenti lo crea
def ensure_bucket_exists(client, bucket_name, retention_policy=None):
    try:
        buckets_api = client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(bucket_name)
        if bucket is None:
            if retention_policy:
                client.buckets_api().create_bucket(bucket_name=bucket_name, retention_rules=retention_policy)
            else:
                client.buckets_api().create_bucket(bucket_name=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        print(f"Failed to create or find bucket '{bucket_name}': {e}")



# Funzione per processare e scrivere i messaggi su InfluxDB
def process_message(message, influxdb_client, sensor_info):
    tags = {tag: message.get(tag, "") for tag in sensor_info['tags']}

    fields = {}

    for field in sensor_info['fields_to_store']:
        keys = field.split('.')
        value = message['data']
        for key in keys:
            value = value[key]
            if value is None:
                break
        if value is not None:
            fields[field.replace('.', '_')] = value

    try:
        point = Point(sensor_info['measurement'])
        
        for tag_key, tag_value in tags.items():
            point = point.tag(tag_key, tag_value)
        for field_key, field_value in fields.items():
            point = point.field(field_key, field_value)
        point = point.time(datetime.strptime(message['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ"))

    except Exception as e:
        print(f"Failed to create point to InfluxDB: {e}")

    try:
        write_api = influxdb_client.write_api(write_options=WriteOptions(batch_size=1))
        write_api.write(sensor_info['bucket'], org=db_config['influxdb']['org'], record=point)
        print(f"Data written to InfluxDB bucket '{sensor_info['bucket']}' successfully.")
    
    except Exception as e:
        print(f"Failed to write data to InfluxDB: {e}")



# Funzione per consumare i messaggi da Kafka
def consume_kafka_messages(sensor_info, process_message):
    broker = sensor_info['broker']
    port = sensor_info['port']
    topic = sensor_info['topic']

    url = f'{broker}:{port}'

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=url,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        #group_id='sensor-monitor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        process_message(message.value["payload"])



# Funzione per avviare il thread del consumatore
def start_consumer_thread(sensor_info, influxdb_client):
    consume_kafka_messages(sensor_info, lambda msg: process_message(msg, influxdb_client, sensor_info))



if __name__ == "__main__":
    sensor_config_file = './VOL200GB/framework/sensors_config.json'
    db_config_file = './VOL200GB/framework/db_config.json'

    sensor_config = load_sensor_config(sensor_config_file)
    db_config = load_db_config(db_config_file)

    influxdb_client = create_influxdb_client(db_config['influxdb'])

    if influxdb_client:
        print(f"Connected to InfluxDB at {db_config['influxdb']['address']}")
    

    threads = []
    for sensor_id, sensor_info in sensor_config.items():
        ensure_bucket_exists(influxdb_client, sensor_info['bucket'])
    
    for sensor_id, sensor_info in sensor_config.items():
        sensor_info['sensor_id'] = sensor_id
        print(f"{sensor_id} {sensor_info}")

        thread = threading.Thread(target=start_consumer_thread, args=(sensor_info, influxdb_client))
        thread.start()
        threads.append(thread)
        print(f"Started thread for sensor: {sensor_id}")

    try:
        threading.Event().wait() 
    except KeyboardInterrupt:
        print("Interrupted by user, shutting down.")
        for thread in threads:
            thread.join()
    print("All threads have finished.")