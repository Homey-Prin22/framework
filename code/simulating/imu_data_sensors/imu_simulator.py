import threading
import time
from imu_unit import IMUUnit
from mqtt_client import MQTTClient
import mqtt_config as config

class IMUSimulator:
    def __init__(self, mqtt_client, topic, quaternion_frequency_hz):
        self.mqtt_client = mqtt_client
        self.topic = topic
        self.quaternion_frequency_hz = quaternion_frequency_hz
        self.imu_units = []

    def add_imu_unit(self, sensor_id, position):
        imu_unit = IMUUnit(sensor_id, position, self.quaternion_frequency_hz)
        self.imu_units.append(imu_unit)

    def start_simulation(self):
        self.mqtt_client.connect()
        threads = []
        for imu_unit in self.imu_units:
            thread = threading.Thread(target=self.send_imu_data, args=(imu_unit,))
            threads.append(thread)
            thread.start()

    def send_imu_data(self, imu_unit):
        interval = 1.0 / imu_unit.quaternion_frequency_hz
        while True:
            imu_data_json = imu_unit.get_data()
            print(imu_data_json)
            subtopic = f"{self.topic}/{imu_unit.position}"
            self.mqtt_client.publish(subtopic, imu_data_json)
            time.sleep(interval)
            
# Configurazione della frequenza (Hz) per i quaternioni
quaternion_frequency_hz = 1

# Creazione del client MQTT
mqtt_client = MQTTClient(config.MQTT_BROKER, config.MQTT_PORT)

# Creazione del simulatore IMU
imu_simulator = IMUSimulator(mqtt_client, config.MQTT_TOPIC, quaternion_frequency_hz)

# Aggiunta delle unità IMU al simulatore
imu_simulator.add_imu_unit("imu_unit_1", "wrist")
imu_simulator.add_imu_unit("imu_unit_2", "forearm")
imu_simulator.add_imu_unit("imu_unit_3", "upperarm")

# Avvio della simulazione
imu_simulator.start_simulation()
