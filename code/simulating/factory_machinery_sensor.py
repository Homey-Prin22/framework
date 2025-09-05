import random
import time
import json
import threading
import paho.mqtt.client as mqtt

machine_ids = [1,2,3,4,5,6]

# MQTT broker configuration
mqtt_broker = "localhost"
mqtt_port = 1883
mqtt_topic_env = "factory/machinery/"

# MQTT client initialization
mqtt_client = mqtt.Client()
mqtt_client.connect(mqtt_broker, mqtt_port, 60)

# Boolean to check if loop in execution
running = True

def basevalue_temp_sup(id):
    if id == 1:
        base_value = random.uniform(30, 40)
    elif id in [2, 3, 6]:
        base_value = random.uniform(25, 35)
    elif id in [4, 5]:
        base_value = random.uniform(25, 40)
    else:
        raise ValueError(f"ID {id} non riconosciuto")
    return round(base_value, 2)

def basevalue_rumore(id):
    if id == 1:
        base_value = random.uniform(90, 110)
    elif id in [2, 3]:
        base_value = random.uniform(85, 105)
    elif id in [4, 5]:
        base_value = random.uniform(95, 105)
    elif id == 6:
        base_value = random.uniform(90, 105)
    else:
        raise ValueError(f"ID {id} non riconosciuto")
    return round(base_value, 2)

def basevalue_vel_rot(id):
    if id in [1, 6]:
        base_value = random.uniform(75, 85)
    elif id == 2:
        base_value = random.uniform(70, 80)
    elif id == 3:
        base_value = random.uniform(65, 80)
    elif id in [4, 5]:
        base_value = random.uniform(180, 220)
    else:
        raise ValueError(f"ID {id} non riconosciuto")
    return round(base_value, 2)

def basevalue_vibra(id):
    if id in [1, 4, 5, 6]:
        base_value = random.uniform(0.2, 1.0)
    elif id in [2, 3]:
        base_value = random.uniform(0.1, 0.8)
    else:
        raise ValueError(f"ID {id} non riconosciuto")
    return round(base_value, 2)



# Dicts to keep track of previous parameters values
temperatura_motore = {id: random.uniform(40, 120) for id in machine_ids}

temperatura_superficiale = {id: basevalue_temp_sup(id) for id in machine_ids}

rumore = {id: basevalue_rumore(id) for id in machine_ids}

vel_rotazione = {id: basevalue_vel_rot(id) for id in machine_ids}

vibra = {id: basevalue_vibra(id) for id in machine_ids}

# Functions
def simulate_temp_motore(id):
    prev_value = temperatura_motore[id]
    new_value = max(40, min(120, round(random.uniform(prev_value - 1, prev_value + 1), 2)))
    temperatura_motore[id] = new_value
    return new_value

def simulate_temp_sup(id):
    prev_value = temperatura_superficiale[id]
    new_value = max(25, min(40, round(random.uniform(prev_value - 1, prev_value + 1), 2)))
    temperatura_superficiale[id] = new_value
    return new_value

def simulate_rumore(id):
    prev_value = rumore[id]
    new_value = max(85, min(110, round(random.uniform(prev_value - 1, prev_value + 1), 2)))
    rumore[id] = new_value
    return new_value

def simulate_vel_rot(id):
    prev_value = vel_rotazione[id]
    new_value = max(65, min(220, round(random.uniform(prev_value - 2, prev_value + 2), 2)))
    vel_rotazione[id] = new_value
    return new_value

def simulate_vibr(id):
    prev_value = vibra[id]
    new_value = max(0.1, min(1.0, round(random.uniform(prev_value - 0.1, prev_value + 0.1), 2)))
    vel_rotazione[id] = new_value
    return new_value



def generate_environment_data():
    while running:
        timestamp = int(time.time()*1000)  # Timestamp in milliseconds
        for id in machine_ids:
            temp_motore = simulate_temp_motore(id)
            temp_superficie = simulate_temp_sup(id)
            rumore = simulate_rumore(id)
            vel_rotazione = simulate_vel_rot(id) 
            vibrazioni = simulate_vibr(id)

            mqtt_topic = str(mqtt_topic_env+str(id))

            data_row = {
                'machine_id': id,
                'temp_motore': temp_motore,
                'temp_superficie': temp_superficie,
                'rumore': rumore,
                'vel_rotazione': vel_rotazione,
                'vibrazioni': vibrazioni,
                'timestamp' : timestamp
                #'percentuale_esec': ,
                #'fase': 
            }

            mqtt_client.publish(mqtt_topic, json.dumps(data_row))

        time.sleep(5)

# Start loop
try:
    generate_environment_data()
except KeyboardInterrupt:
    running = False 
finally:
    mqtt_client.disconnect()
