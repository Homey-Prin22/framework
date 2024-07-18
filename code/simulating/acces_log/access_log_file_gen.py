import random
import time
from datetime import datetime

NUM_USERS = 50 
NUM_ROOMS = 10
n_sec = 5
LOG_FILE = '/VOL200GB/framework/monitoring/access_logs.txt'
MESSAGE_TEMPLATE = "User {user_id} unsuccessfully attempted to access Room {room_id} at {timestamp}."


def generate_message():
    user_id = random.randint(1, NUM_USERS)
    room_id = random.randint(1, NUM_ROOMS)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return MESSAGE_TEMPLATE.format(user_id=user_id, room_id=room_id, timestamp=timestamp)

def generate_access_logs():
    while True:
        message = generate_message()
        print(message) 
        
        with open(LOG_FILE, 'a') as file:
            file.write(message + '\n')
        
        time.sleep(random.uniform(1, n_sec))

if __name__ == "__main__":
    generate_access_logs()
