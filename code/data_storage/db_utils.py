import json
import psycopg2
import mysql.connector
from influxdb_client import InfluxDBClient
from pymongo import MongoClient
from mysql.connector import Error


def load_db_config(config_file='/framework/code/db_config.json'):
    with open(config_file, 'r') as f:
        return json.load(f)


def connection_to_influxdb():
    db_config = load_db_config()
    db_info = db_config["influxdb"]
    try:
        client = InfluxDBClient(
            url=db_info['address'],
            token=db_info['token'],
            org=db_info['org'],
        )
        print("Connected to InfluxDB successfully.")
        return client
    except Exception as e:
        print(f"Failed to connect to InfluxDB: {e}")


def connection_to_mysql():
    db_config = load_db_config()
    db_info = db_config["mysql"]

    connection = None
    try:
        connection = mysql.connector.connect(
            host=db_info['MYSQL_HOST'],
            user=db_info['MYSQL_USER'],
            password=db_info['MYSQL_PASSWORD'],
            database=db_info['MYSQL_DATABASE']
        )

        if connection.is_connected():
            print("Connected to MySQL successfully.")
            return connection
    except Error as e:
        print(f"Failed to connect to MySQL: {e}")
    

def connection_to_mongodb(mongo_collection):
    db_config = load_db_config()
    db_info = db_config["mongodb"]

    try:
        client = MongoClient(db_info["MONGO_URI"])
        client.admin.command("ping") 
        db = client[db_info["MONGO_DB"]]
        collection = db[mongo_collection]
        print(f"Connected to MongoDB collection {mongo_collection} successfully.")
        return collection
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")


def connection_to_postgres():
    db_config = load_db_config()
    db_info = db_config["postgres"]
    try:
        connection = psycopg2.connect(
            database=db_info["DB_NAME"],
            user=db_info["DB_USER"],
            password=db_info["DB_PASS"],
            host=db_info["DB_HOST"],
            port=db_info["DB_PORT"]
        )
        print("Connected to PostgreSQL successfully.")
        return connection
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")