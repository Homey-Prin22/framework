import json
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime, timedelta
import threading
import random

# Configurazione broker Kafka
KAFKA_BROKER = '192.168.104.78:9092'
KAFKA_TOPIC = 'task_topic'

# Funzione per caricare la configurazione del database dal file JSON
def load_db_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)['mysql']

def create_connection(db_config_file):

    db_config = load_db_config(db_config_file)

    connection = None
    try:
        connection = mysql.connector.connect(
            host=db_config['MYSQL_HOST'],
            user=db_config['MYSQL_USER'],
            password=db_config['MYSQL_PASSWORD'],
            database=db_config['MYSQL_DATABASE']
        )

        if connection.is_connected():
            print("Successfully connected to MySQL")
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    return connection

def insert_task(connection, task):
    insert_query = """
    INSERT INTO tasks (task_id, user_id, room_id, priority, due_date, description, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor = connection.cursor()
    cursor.execute(insert_query, (
        task['task_id'], 
        task['user_id'], 
        task['room_id'], 
        task['priority'], 
        task['due_date'], 
        task['description'],
        task['created_at']
    ))
    connection.commit()

def consume_tasks():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    connection = create_connection()
    if not connection:
        return

    try:
        for message in consumer:
            task = message.value
            print(f"Consuming task: {task}")
            insert_task(connection, task)
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        if connection.is_connected():
            connection.close()
            print("Connection to MySQL closed")

if __name__ == "__main__":
    db_config_file = './VOL200GB/framework/db_config.json'
    consume_tasks(db_config_file)
