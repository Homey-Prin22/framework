import requests
import sseclient
import json

def fetch_sensor_data(site):
    url = f'http://192.168.104.78:5000/api/data?site={site}'
    print(f"Fetching data from URL: {url}")

    try:
        response = requests.get(url, stream=True)
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return

    if response.status_code != 200:
        print(f"Failed to fetch data. Received unexpected status code: {response.status_code}")
        return
    else:
        client = sseclient.SSEClient(url)
        #print(client)
        
        try:
            for event in client:
                print(event.data)
        except Exception as e:
            print(f"An error occured while processing events: {e}")

if __name__ == "__main__":
    site = input("Enter the site name: ")
    fetch_sensor_data(site)


'''
        try:
            for event in client.events():
                print(f"Event: {event.data}")
            data = json.loads(event.data)
            print(f"New data received: {data}")
    '''
