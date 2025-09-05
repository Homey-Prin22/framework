import random
import time
import json
import threading
import paho.mqtt.client as mqtt

rooms = ['kitchen', 'living_room', 'bedroom', 'bathroom', 'dining_room', 'garage', 'garden']

# MQTT broker configuration
mqtt_broker = "localhost"
mqtt_port = 1883
mqtt_topic_env = "environment/"

# MQTT client initialization
mqtt_client = mqtt.Client()
mqtt_client.connect(mqtt_broker, mqtt_port, 60)

# Boolean to check if loop in execution
running = True

def basevalue_pollution(room):
    for room in rooms:
        if room in ['kitchen', 'garage']:
            base_value = random.uniform(100, 200) 
        elif room == 'garden':
            base_value = random.uniform(10, 50)
        else:
            base_value = random.uniform(50, 100)
        return round(base_value, 2)

# Dicts to keep track of previous temperature, humidity and pollution values
temperature_data = {room: random.uniform(18.0, 22.0) for room in rooms}
humidity_data = {room: random.uniform(45.0, 55.0) for room in rooms}
pollution_data = {room: basevalue_pollution(room) for room in rooms}


# Functions
def simulate_temperature(room):
    prev_value = temperature_data[room]
    new_value = max(0, min(40, round(random.uniform(prev_value - 0.5, prev_value + 0.5), 2)))
    temperature_data[room] = new_value
    return new_value

def simulate_humidity(room):
    prev_value = humidity_data[room]
    new_value = max(0, min(100, round(random.uniform(prev_value - 2.0, prev_value + 2.0), 2)))
    humidity_data[room] = new_value
    return new_value

def simulate_pollution(room):
    prev_value = pollution_data[room]
    new_value = max(0, min(500, round(random.uniform(prev_value - 10.0, prev_value + 10.0), 2)))
    pollution_data[room] = new_value
    return new_value

def simulate_rssi():
    return round(random.uniform(-90, -30), 2)

def generate_environment_data(sensor_id):
    while running:
        timestamp = int(time.time()*1000)  # Timestamp in milliseconds
        for room in rooms:
            temperature = simulate_temperature(room)
            humidity = simulate_humidity(room)
            air_pollution = simulate_pollution(room)
            rssi = simulate_rssi() 

            sensor_name = str(sensor_id+room)
            mqtt_topic = str(mqtt_topic_env+room)

            data_row = {
                'sensor_id': sensor_name,
                'room': room,
                'timestamp': timestamp,
                'temperature': temperature,
                'humidity': humidity,
                'air_particles': air_pollution,
                'rssi': rssi
            }

            mqtt_client.publish(mqtt_topic, json.dumps(data_row))

        time.sleep(5)

sensor_id = 'sensor_mix_'

# Start loop 
try:
    generate_environment_data(sensor_id)
except KeyboardInterrupt:
    running = False 
finally:
    mqtt_client.disconnect()
