import random
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configurazione produttore Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

USER_IDS = 50
ROOM_IDS = 10
PRIORITIES = ['low', 'medium', 'high']
#NUM_TASKS = 10 
TOPIC = 'task_topic'

def generate_random_task():
    user_id = random.randint(1, USER_IDS)
    room_id = random.randint(1, ROOM_IDS)
    priority = random.choice(PRIORITIES)
    due_date = (datetime.now() + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
    task_id = f"task_{random.randint(1000, 9999)}"
    description = f"Task {task_id} description"
    created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    task = {
        'task_id': task_id,
        'user_id': user_id,
        'room_id': room_id,
        'priority': priority,
        'due_date': due_date,
        'description': description,
        'created_at': created_at
    }

    return task

def send_tasks_to_kafka():
    while True:
        task = generate_random_task()
        producer.send(TOPIC, task)
        print(f"Sent task: {task}")
        time.sleep(random.uniform(0.5, 2.0)) 

if __name__ == "__main__":
    send_tasks_to_kafka()
    producer.flush() 
