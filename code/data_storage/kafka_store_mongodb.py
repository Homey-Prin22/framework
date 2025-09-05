import pymongo
import json
import threading
from kafka import KafkaConsumer
from pymongo import MongoClient
from kafka_consumer import kafka_consumer
from db_utils import connection_to_mongodb

# Dizionario con topic Kafka come chiavi e collezioni MongoDB come valori
kafka_to_mongo = {
    "factory_machinery_1": "factory_machinery_1",
    "factory_machinery_2": "factory_machinery_2",
    "factory_machinery_3": "factory_machinery_3",
    "factory_machinery_4": "factory_machinery_4",
    "factory_machinery_5": "factory_machinery_5",
    "factory_machinery_6": "factory_machinery_6"
}

def consume_and_store(kafka_topic, mongo_collection):

    collection = connection_to_mongodb(mongo_collection)

    consumer = kafka_consumer(kafka_topic)

    for message in consumer:
        try:
            msg = message.value
            result = collection.insert_one(msg)
            print(f"Message stored with ID: {result.inserted_id}")
        except Exception as e:
            print(f"Error: {e}")

# Start threads
threads = []
for kafka_topic, mongo_collection in kafka_to_mongo.items():
    thread = threading.Thread(target=consume_and_store, args=(kafka_topic, mongo_collection))
    threads.append(thread)
    thread.start()

# Wait until threads stop working
for thread in threads:
    thread.join()