import json
import threading
from flask import Flask, request, Response, jsonify
from kafka_consumer import load_sensor_config, get_sensor_info, get_sensors_in_site, create_kafka_consumer, filter_message, consume_sensor_data

app = Flask(__name__)

# Endpoint per ottenere i dati del sensore
@app.route('/api/data', methods=['GET'])
def get_data():
    site = request.args.get('site')
    print(site)

    if not site:
        return jsonify({"error": "Site is required"}), 400

    sensor_config = load_sensor_config('sensors_config.json')
    sensors_in_site = get_sensors_in_site(sensor_config, site)

    for sensor in sensors_in_site:
        print(sensor)
    
    if not sensors_in_site:
        return jsonify({"error": f"No sensors found in site {site}"}), 404
    '''
    def generate():
        for sensor in sensors_in_site:
            for filtered_message in consume_sensor_data(sensor):
                yield f"data: {json.dumps(filtered_message)}\n\n"
        
        #for filtered_message in consume_sensor_data(sensor_info):
            #yield f"data: {json.dumps(filtered_message)}\n\n"

    return Response(generate(), content_type='text/event-stream')
    '''

    def generate():
        stop_event = threading.Event()
        message_queue = []
        threads = []

        def thread_target(sensor_info):
            consume_sensor_data(sensor_info, site, stop_event, message_queue)
        
        for sensor in sensors_in_site:
            
            thread = threading.Thread(target=thread_target, args=(sensor,))
            thread.start()
            threads.append(thread)

        try:
            while any(thread.is_alive for thread in threads):
                while message_queue:
                    yield f"data: {json.dumps(message_queue.pop(0))}\n\n"
        finally:
            stop_event.set()
            for thread in threads:
                thread.join()
            
    return Response(generate(), content_type='text/event-stream')

if __name__ == '__main__':
    app.run(host='192.168.104.78', port=5000)
