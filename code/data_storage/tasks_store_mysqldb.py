import json
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error
from kafka_consumer import kafka_consumer
from db_utils import connection_to_mysql

KAFKA_TOPIC = 'task_topic'


def insert_task(connection, task):
    insert_query = """
    INSERT INTO tasks (task_id, user_id, room_id, priority, due_date, description, created_at, completed_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor = connection.cursor()
    cursor.execute(insert_query, (
        task['task_id'], 
        task['user_id'], 
        task['room_id'], 
        task['priority'], 
        task['due_date'], 
        task['description'],
        task['created_at'],
        task['completed_at']
    ))
    connection.commit()

def consume_tasks():
    consumer = kafka_consumer(KAFKA_TOPIC)
    connection = connection_to_mysql()

    try:
        for message in consumer:
            task = message.value
            insert_task(connection, task)
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        if connection.is_connected():
            connection.close()
            print("Connection to MySQL closed")

if __name__ == "__main__":
    consume_tasks()
