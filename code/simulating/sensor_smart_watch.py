import random
import time
import json
from kafka import KafkaProducer

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "test_smart_watch_gesture"

# Initial values for each sensor
prev_values = {
    "accelerometer": {"x": 0.0, "y": 0.1, "z": 0.0},
    "gyroscope": {"x": 0.0, "y": 0.0, "z": 0.0},
    "magnetometer": {"x": 950.0, "y": -630.0, "z": -900.0},
    "gravity": {"x": 0.5, "y": 0.0, "z": 9.8},
    "compass": {"value": 110.0},
    "humidity": {"value": 40.0},
    "temperature": {"value": 20.0},
    "barometer": {"value": 998.0},
    "altimeter": {"value": 120.0},
    "lightSensor": {"value": 300.0}
}

def update_value(prev, min_val, max_val, delta=0.1):
    new_val = prev + random.uniform(-delta, delta)
    return round(max(min_val, min(max_val, new_val)), 8)

def generate_sensor_data(timestamp):
    global prev_values  # variable to keep status

    sensor_data = {
        "timestamp": timestamp,
        "accelerometer": {
            "x": update_value(prev_values["accelerometer"]["x"], -0.6, 0.5, 0.2),
            "y": update_value(prev_values["accelerometer"]["y"], -2, 0.5, 0.2),
            "z": update_value(prev_values["accelerometer"]["z"], -1.5, 0.5, 0.2)
        },
        "gyroscope": {
            "x": update_value(prev_values["gyroscope"]["x"], -0.8, 2, 0.3),
            "y": update_value(prev_values["gyroscope"]["y"], -0.4, 0.3, 0.3),
            "z": update_value(prev_values["gyroscope"]["z"], -0.5, 0.3, 0.3)
        },
        "magnetometer": {
            "x": update_value(prev_values["magnetometer"]["x"], 900, 999, 5),
            "y": update_value(prev_values["magnetometer"]["y"], -630, -620, 2),
            "z": update_value(prev_values["magnetometer"]["z"], -950, -850, 3)
        },
        "gravity": {
            "x": update_value(prev_values["gravity"]["x"], -6, 3, 0.5),
            "y": update_value(prev_values["gravity"]["y"], -3, 6, 0.5),
            "z": update_value(prev_values["gravity"]["z"], 4.5, 10, 0.3)
        },
        "compass": {
            "value": update_value(prev_values["compass"]["value"], -130, 30, 5)
        },
        "pose": {
            "x": 0.0, "y": 0.0, "z": 0.0,
            "qx": 0.0, "qy": 0.0, "qz": 0.0, "qw": 0.0
        },
        "humidity": {
            "value": update_value(prev_values["humidity"]["value"], 30, 70, 2)
        },
        "temperature": {
            "value": round(update_value(prev_values["temperature"]["value"], 15, 25, 0.2), 5)
        },
        "barometer": {
            "value": round(update_value(prev_values["barometer"]["value"], 997, 999, 0.1), 5)
        },
        "altimeter": {
            "value": round(update_value(prev_values["altimeter"]["value"], 100, 130, 0.3), 5)
        },
        "lightSensor": {
            "value": update_value(prev_values["lightSensor"]["value"], 100, 700, 10)
        }
    }

    # update previous values
    for key in prev_values:
        if isinstance(prev_values[key], dict):
            for subkey in prev_values[key]:
                prev_values[key][subkey] = sensor_data[key][subkey]

    return sensor_data


# send data
while True:
    timestamp = int(time.time() * 1000) # timestamp in milliseconds
    payload = {
        "sensorData": generate_sensor_data(timestamp)
    }

    message = {
        "data": json.dumps(payload)
    }

    producer.send(topic, value=message)

    time.sleep(1)
