import json
import threading
import collections
from flask import Flask, request, Response, jsonify
from kafka_consumer import consume_sensor_data
import utils
import queue

app = Flask(__name__)


# Endpoint to monitor sensor data
@app.route('/api/data', methods=['GET'])
def get_data():
    environ = request.environ.copy()
    args_dict = request.args.to_dict(flat=False)
    sensor_list = []
    username = ""
    role = ""
    if "username" in args_dict:
        username = args_dict["username"]
        #role = utils.getRole(username)      PER RUOLO
    if "source_id" in args_dict:
        sensor_ids = args_dict["source_id"]
        # filter out sensors for which the user role is not authorized PER RUOLO
        #checked_ids = utils.check_Sensor_by_role(role)   PER RUOLO
        # retrieve topic names
        s = utils.get_Sensor_Info_by_IDs(sensor_ids)
        #return jsonify(sensor_ids), 200
        if s!=None:
            sensor_list = s
        else:
            return jsonify({"error": "No data for the specified source_id"}), 400
            
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
            # filter out sensors for which the user role is not authorized
            #checked_sensor_list = utils.check_Sensor_by_role(s,role)     PER RUOLO
            sensor_list = s
        else:
            jsonify({"error": "No data for the specified site(s) and measure(s)"}), 400
    else:
        return jsonify({"error": "Argument 'id', 'site' or 'measure' are required"}), 400

    # Starts a set of threads from the sensor_list   
    def generate(sensor_list, environ):
        stop_event = threading.Event()
        message_queue = queue.Queue(maxsize=100) #collections.deque(maxlen=100) 
        threads = []

        def thread_target(sensor_info):
            consume_sensor_data(sensor_info, stop_event, message_queue)
        
        for sensor in sensor_list:
            thread = threading.Thread(target=thread_target, args=(sensor,))
            thread.daemon = True
            thread.start()
            threads.append(thread)


        try:
            while True:
                try:
                    elem = message_queue.get(timeout=15)
                    yield f"data: {json.dumps(elem)}\n\n"
                except queue.Empty: 
                    #yield ": keep-alive\n\n" 
                    # Check if the client is still connected
                    if environ.get('wsgi.errors'):  
                        try:
                            request.environ.get('wsgi.errors').flush()
                        except Exception:
                            break  # client disconnected
        finally:
            stop_event.set()
            for thread in threads:
                thread.join()

    # Response to the GET /api/data     
    return Response(generate(sensor_list,environ), content_type='text/event-stream')
        
        
if __name__ == '__main__':
    app.run(host='localhost', port=5000)
