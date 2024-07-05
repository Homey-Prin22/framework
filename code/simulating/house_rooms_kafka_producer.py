import random
import time
from kafka import KafkaProducer
import json
import threading, csv
import os

rooms = ['kitchen', 'living room', 'bedroom', 'bathroom', 'dining room']
sensors = {room: i+1 for i, room in enumerate(rooms)}
floor = random.randint(1, 2)

# Producer per Kafka
kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                               value_serializer=lambda x:
                               json.dumps(x).encode('utf-8'))

# Variabile per controllare se i thread devono essere in esecuzione
running = True

def generate_data(measure):

        while running:
            for room in rooms:
                timestamp = int(time.time())
                
                # Controllo temperatura
                if measure == 'temperature':
                    # Simula una piccola variazione rispetto al valore precedente
                    prev_value = temperature_data.get(room, 20.0)  # Valore predefinito
                    value = max(0, min(40, round(random.uniform(prev_value - 0.5, prev_value + 0.5), 2)))
                    temperature_data[room] = value
                elif measure == 'humidity':
                    # Controllo umidità
                    prev_value = humidity_data.get(room, 55.0)  # Valore predefinito
                    value = max(0, min(100, round(random.uniform(prev_value - 2.0, prev_value + 2.0), 2)))
                    humidity_data[room] = value

                data_row = {
                    'sensor_id': sensors[room],
                    'room': room,
                    'floor': floor,
                    'timestamp': timestamp,
                    'measure': measure,
                    'value': value
                }

                print(data_row)

                # Invia i dati a Kafka
                kafka_producer.send(measure, value=data_row)

            time.sleep(5 if measure == 'temperature' else 10)

# Dizionari per tenere traccia dei valori precedenti di temperatura e umidità per ogni stanza
temperature_data = {}
humidity_data = {}


# Avvia i thread per la generazione dei dati
temperature_thread = threading.Thread(target=generate_data, args=('temperature',))
humidity_thread = threading.Thread(target=generate_data, args=('humidity',))

temperature_thread.start()
humidity_thread.start()

# Attendi fino a quando l'applicazione viene interrotta
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    running = False  # Interrompi i thread quando l'applicazione viene chiusa

# Attendi che i thread terminino
temperature_thread.join()
humidity_thread.join()
