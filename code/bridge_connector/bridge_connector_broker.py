import paho.mqtt.client as mqtt
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer
import json
from kafka.admin import NewTopic
from datetime import datetime, timezone
import time

def load_config(file_path):
    with open(file_path) as config_file:
        return json.load(config_file)


def create_kafka_admin_client(bootstrap_servers):
    return KafkaAdminClient(bootstrap_servers=bootstrap_servers)


def ensure_kafka_topics(admin_client, sensors, bootstrap_servers):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    existing_topics = consumer.topics()
    consumer.close()

    topics_to_create = []

    for sensor_id, sensor in sensors.items():
        kafka_topics = get_kafka_topics(sensor)
        for kafka_topic in kafka_topics:
            if kafka_topic not in existing_topics:
                topics_to_create.append(NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1))
                print(f"Topic {kafka_topic} added to to-create list.")

    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print(f"Creating following topics: {[topic.name for topic in topics_to_create]}")


def create_kafka_producer(bootstrap_servers):
    return KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def get_kafka_topics(sensor):
    if 'kafka_topic' in sensor:
        return [sensor['kafka_topic']]
    elif 'kafka_topics' in sensor:
        return list(sensor['kafka_topics'].values())
    return []


def subscribe_to_mqtt_topics(client, sensors):
    unique_topics = set()

    for sensor_id, sensor_info in sensors.items():
        if 'mqtt_topic' in sensor_info:
            mqtt_topic = sensor_info.get('mqtt_topic')
            unique_topics.add(sensor_info['mqtt_topic'])

    unique_topics_list = list(unique_topics)
    print(unique_topics_list)

    for topic in unique_topics:
        client.subscribe(topic)
        print(f"Subscribed to topic {topic}")

# Funzione di callback per la connessione a MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Successfully connected to broker MQTT.")
        subscribe_to_mqtt_topics(client, userdata['sensors'])
    else:
        print(f"Error while connecting to broker MQTT. Error code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload_json = json.loads(msg.payload.decode('utf-8'))
        sensor = get_sensor_config_for_mqtt_topic(msg.topic, userdata['sensors'])
        if sensor:
            process_and_forward_message(sensor, msg.topic, payload_json, userdata['kafka_producer'])
    except Exception as e:
        print(f"Error while processing message: {e}")


def get_sensor_config_for_mqtt_topic(mqtt_topic, sensors):
    for sensor_id, sensor in sensors.items():
        if sensor.get('mqtt_topic') == mqtt_topic:
            return sensor
    return None

def process_and_forward_message(sensor, mqtt_topic, payload_json, kafka_producer):

    if 'kafka_topic' in sensor:
        kafka_topic = sensor['kafka_topic']
        kafka_message = payload_json

        kafka_producer.send(kafka_topic, value=kafka_message)
        #print(f"Message sent to Kafka topic {kafka_topic}: {kafka_message}")
    else:
        print(f"No Kafka topic for MQTT topic {mqtt_topic}")                    

# Carica la configurazione
config = load_config('/framework/code/bridge_connector/sensors_config.json')

# Crea un client Kafka per l'amministrazione
admin_client = create_kafka_admin_client(config['kafka_broker']['bootstrap_servers'])

# Verifica e crea i topic Kafka se necessario
#ensure_kafka_topics(admin_client, config['sensors'])
ensure_kafka_topics(admin_client, config['sensors'], config['kafka_broker']['bootstrap_servers'])

# Crea il producer Kafka
kafka_producer = create_kafka_producer(config['kafka_broker']['bootstrap_servers'])

# Dizionario per tracciare i timestamp dei messaggi
message_timestamps = {}

# Crea il client MQTT e configura i callback
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.user_data_set({'sensors': config['sensors'], 'kafka_producer': kafka_producer})
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connessione al broker MQTT
mqtt_broker = config['mqtt_broker']
mqtt_client.connect(mqtt_broker['host'], mqtt_broker['port'], 60)

# Avvia il loop del client MQTT
try:
    mqtt_client.loop_forever()
except KeyboardInterrupt:
    print("Interrupted by user, shutting down.")