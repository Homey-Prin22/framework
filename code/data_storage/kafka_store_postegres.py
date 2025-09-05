import psycopg2
import json
import threading
from kafka import KafkaConsumer
from db_utils import connection_to_postgres
from kafka_consumer import kafka_consumer

def process_table(table_name, kafka_topic):

    # Initializing cursor connection to PostegreSQL db
    connection = connection_to_postgres()
    cursor = connection.cursor()

    # Initializing Kafka consumer
    consumer = kafka_consumer(kafka_topic)

    print(f"Worker for table {table_name} and topic {kafka_topic} started.")

    # Consume messages from kafka topic and insert them into table
    for message in consumer:
        msg = message.value
        cursor.execute(f"""
            INSERT INTO {table_name} (sensor_id, room, timestamp, temperature, humidity, air_particles, rssi)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (msg['sensor_id'], msg['room'], msg['timestamp'], msg['temperature'], msg['humidity'], msg['air_particles'], msg['rssi']))
        connection.commit()

    cursor.close()
    connection.close()

# Read configuration tabel - topic
with open('sensormix_tables_config.json', 'r') as file:
    config = json.load(file)

# Start thread for each table
threads = []
for table, topic in config['tables'].items():
    thread = threading.Thread(target=process_table, args=(table, topic))
    threads.append(thread)
    thread.start()

# Wait until threads stop working
for thread in threads:
    thread.join()