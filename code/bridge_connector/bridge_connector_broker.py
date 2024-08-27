import paho.mqtt.client as mqtt
from kafka import KafkaProducer, KafkaAdminClient
import json
from kafka.admin import NewTopic


def load_config(file_path):
    with open(file_path) as config_file:
        return json.load(config_file)


def create_kafka_admin_client(bootstrap_servers):
    return KafkaAdminClient(bootstrap_servers=bootstrap_servers)


def ensure_kafka_topics(admin_client, sensors):
    existing_topics = admin_client.list_topics()
    topics_to_create = []
 
    for sensor_id, sensor in sensors.items():
        kafka_topics = get_kafka_topics(sensor)
        for kafka_topic in kafka_topics:
            if kafka_topic not in existing_topics:
                topics_to_create.append(NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1))
                print(f"Aggiunta del topic {kafka_topic} alla lista di creazione.")

    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print(f"Creati i seguenti topic: {[topic.name for topic in topics_to_create]}")


def create_kafka_producer(bootstrap_servers):
    return KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def get_kafka_topics(sensor):
    if 'kafka_topic' in sensor:
        return [sensor['kafka_topic']]
    elif 'kafka_topics' in sensor:
        return list(sensor['kafka_topics'].values())
    return []


def subscribe_to_mqtt_topics(client, sensors):
    for sensor_id, sensor in sensors.items():
        mqtt_topic = sensor.get('mqtt_topic')
        if mqtt_topic:
            client.subscribe(mqtt_topic)
            print(f"Sottoscritto al topic {mqtt_topic} per il sensore {sensor_id}.")


# Funzione di callback per la connessione a MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connessione al broker MQTT avvenuta con successo.")
        subscribe_to_mqtt_topics(client, userdata['sensors'])
    else:
        print(f"Errore nella connessione al broker MQTT. Codice: {rc}")


def on_message(client, userdata, msg):
    try:
        payload_json = json.loads(msg.payload.decode('utf-8'))
        sensor = get_sensor_config_for_mqtt_topic(msg.topic, userdata['sensors'])
        if sensor:
            process_and_forward_message(sensor, msg.topic, payload_json, userdata['kafka_producer'])
    except Exception as e:
        print(f"Errore nell'elaborazione del messaggio: {e}")


def get_sensor_config_for_mqtt_topic(mqtt_topic, sensors):
    for sensor_id, sensor in sensors.items():
        if sensor.get('mqtt_topic') == mqtt_topic:
            return sensor
    return None


def process_and_forward_message(sensor, mqtt_topic, payload_json, kafka_producer):
    if 'kafka_topic' in sensor:
        kafka_topic = sensor['kafka_topic']
        kafka_message = {
            "topic": mqtt_topic,
            "payload": payload_json
        }
        kafka_producer.send(kafka_topic, value=kafka_message)
        kafka_producer.flush()
        print(f"Messaggio inviato al topic Kafka {kafka_topic}: {kafka_message}")
    elif 'kafka_topics' in sensor:
        for key, kafka_topic in sensor['kafka_topics'].items():
            if key in payload_json:
                kafka_message = {field: payload_json.get(field) for field in sensor['fields_to_monitor']}
                kafka_message["measure"] = key
                kafka_message["value"] = payload_json[key]
                kafka_producer.send(kafka_topic, value=kafka_message)
                kafka_producer.flush()
                print(f"Messaggio inviato al topic Kafka {kafka_topic}: {kafka_message}")
    else:
        print(f"Nessun topic Kafka configurato per il topic MQTT {mqtt_topic}")


# Carica la configurazione
config = load_config('sensors_config.json')

# Crea un client Kafka per l'amministrazione
admin_client = create_kafka_admin_client(config['kafka_broker']['bootstrap_servers'])

# Verifica e crea i topic Kafka se necessario
ensure_kafka_topics(admin_client, config['sensors'])

# Crea il producer Kafka
kafka_producer = create_kafka_producer(config['kafka_broker']['bootstrap_servers'])

# Crea il client MQTT e configura i callback
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.user_data_set({'sensors': config['sensors'], 'kafka_producer': kafka_producer})
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connessione al broker MQTT
mqtt_broker = config['mqtt_broker']
mqtt_client.connect(mqtt_broker['host'], mqtt_broker['port'], 60)

# Avvia il loop del client MQTT
mqtt_client.loop_forever()