import json
import requests

base_url = 'http://localhost:5010/api/utils?username='

sensor_list = []

def get_sensor_list(user, key):
    try:
        url = base_url + str(user)
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json()
        for element in data:
            for k, v in element.items():
                if k==key and v not in sensor_list:
                    sensor_list.append(v)
        return sensor_list
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")