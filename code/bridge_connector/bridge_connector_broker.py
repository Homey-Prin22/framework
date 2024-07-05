import paho.mqtt.client as mqtt
from kafka import KafkaProducer, KafkaAdminClient
import json
from kafka.admin import NewTopic
import os

# Carica la configurazione dal file JSON
with open('bridge_config.json') as config_file:
    config = json.load(config_file)

mqtt_broker = config['mqtt_broker']
kafka_broker = config['kafka_broker']

# Crea un client Kafka per l'amministrazione
admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])



# Verifica e crea i topic Kafka se non esistono
def ensure_kafka_topics(config):
    existing_topics = admin_client.list_topics()
    topics_to_create = []
    
    for topic in config['topics']:
        kafka_topic = topic['kafka_topic']
        if kafka_topic not in existing_topics:
            topics_to_create.append(NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1))
    
    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)

ensure_kafka_topics(config)

# Crea il producer Kafka
kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                               value_serializer=lambda x: json.dumps(x).encode('utf-8'))



# Funzione di callback per la connessione a MQTT
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))
    # Iscriviti a tutti i topic specificati nel file di configurazione
    for topic in config['topics']:
        client.subscribe(topic['mqtt_topic'])

# Funzione di callback per la ricezione dei messaggi da MQTT
def on_message(client, userdata, msg):
    kafka_topic = None
    for topic in config['topics']:
        if topic['mqtt_topic'] == msg.topic:
            kafka_topic = topic['kafka_topic']
            break
           
    if kafka_topic:
        try:
            payload = msg.payload.decode('utf-8')  # Decodifica il payload
            payload_json = json.loads(payload)  # Deserializza il payload in JSON
            kafka_message = {
                "topic": msg.topic,
                "payload": payload_json
            }
            kafka_producer.send(kafka_topic, value=kafka_message)
            kafka_producer.flush()
        except Exception as e:
                print("Error publishing message to Kafka:", e)
    else:
        print(f"No Kafka topic configured for MQTT topic {msg.topic}")


mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(mqtt_broker['host'], mqtt_broker['port'], 60)

# Configura il producer Kafka
#kafka_producer = Producer({"bootstrap.servers": kafka_broker['bootstrap_servers']})
# Configura il producer Kafka con kafka-python

# Avvia il loop del client MQTT
mqtt_client.loop_forever()




