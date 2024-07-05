import json
import random
from datetime import datetime

class IMUUnit:
    def __init__(self, sensor_id, position, quaternion_frequency_hz):
        self.sensor_id = sensor_id
        self.position = position
        self.quaternion_frequency_hz = quaternion_frequency_hz

    def generate_accelerometer_data(self):
        return {
            'x': random.uniform(-1.0, 1.0),
            'y': random.uniform(-1.0, 1.0),
            'z': random.uniform(-1.0, 1.0)
        }

    def generate_gyroscope_data(self):
        return {
            'x': random.uniform(-0.01, 0.01),
            'y': random.uniform(-0.01, 0.01),
            'z': random.uniform(-0.01, 0.01)
        }

    def generate_magnetometer_data(self):
        return {
            'x': random.uniform(-0.5, 0.5),
            'y': random.uniform(-0.5, 0.5),
            'z': random.uniform(-0.5, 0.5)
        }

    def generate_quaternion_data(self):
        quaternion = {
            'x': random.uniform(-1.0, 1.0),
            'y': random.uniform(-1.0, 1.0),
            'z': random.uniform(-1.0, 1.0),
            'w': random.uniform(-1.0, 1.0)
        }
        # Normalizzazione del quaternione
        norm = (quaternion['x']**2 + quaternion['y']**2 + quaternion['z']**2 + quaternion['w']**2) ** 0.5
        quaternion['x'] /= norm
        quaternion['y'] /= norm
        quaternion['z'] /= norm
        quaternion['w'] /= norm
        return quaternion

    def get_data(self):
        imu_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'sensor_id': self.sensor_id,
            'position': self.position,
            'data': {
                'quaternion': self.generate_quaternion_data()
            }
        }
        return json.dumps(imu_data)
