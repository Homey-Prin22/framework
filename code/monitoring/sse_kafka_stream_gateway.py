import json
import threading
from flask import Flask, request, Response, jsonify
#from kafka_consumer import load_sensor_config, get_sensor_info, get_sensors_in_site, create_kafka_consumer, filter_message, consume_sensor_data
from kafka_consumer import load_sensor_config, create_kafka_consumer, consume_sensor_data
import utils

app = Flask(__name__)


# Endpoint per ottenere i dati del sensore
@app.route('/api/data', methods=['GET'])
def get_data():
    args_dict = request.args.to_dict(flat=False)
    sensor_list = []
    if "sensor_id" in args_dict:
        sensor_ids = args_dict["sensor_id"]
        s = utils.get_Sensor_Info_by_IDs(sensor_ids)
        if s!=None:
            sensor_list = s
        else:
            return jsonify({"error": "No data for the specified sensor_id"}), 400
            
    elif "site" in args_dict or "measure" in args_dict:
        site = []
        measure = []
        if "site" in args_dict:
            site = args_dict["site"]
            #print(site)
        if "measure" in args_dict:
            measure = args_dict["measure"]
        s = []
        s = utils.get_Sensor_Info_by_filter(site,measure)
        if s!=None and len(s)>0:
            sensor_list = s
        else:
            jsonify({"error": "No data for the specified site(s) and measure(s)"}), 400
    else:
        return jsonify({"error": "Argument 'id', 'site' or 'measure' are required"}), 400

    #sensor_config = load_sensor_config('sensors_config.json') # to remove
    # sensor_config = load_sensor_config('sensors_config.json') # to remove
    #sensor_list = get_sensors_in_site(sensor_config, site)

    #for sensor in sensors_in_site:
    #    print(sensor)
    
    #if not sensors_in_site:
    #    return jsonify({"error": f"No sensors found in site {site}"}), 404


        
    def generate(sensor_list):
        stop_event = threading.Event()
        message_queue = []
        threads = []

        def thread_target(sensor_info):
            #consume_sensor_data(sensor_info, site, stop_event, message_queue)
            consume_sensor_data(sensor_info, stop_event, message_queue)
        
        for sensor in sensor_list:
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
            
    return Response(generate(sensor_list), content_type='text/event-stream')
        
        
if __name__ == '__main__':
    app.run(host='192.168.104.78', port=5000)
