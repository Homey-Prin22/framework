import json
from flask import Flask, request, Response, jsonify
import utils


app = Flask(__name__)


# Endpoint to monitor sensor data
@app.route('/api/utils', methods=['GET'])
def get_data():
    args_dict = request.args.to_dict(flat=True)
    username = ""
    sensor_list = []
    if "username" in args_dict:
        username = args_dict["username"]
        if "source_id" in args_dict:
            source_id = args_dict["source_id"]
            s = utils.get_Sensors_schema(source_id,username)
        elif "schema" in args_dict:
            schema = args_dict["schema"]
            if schema=="true":
                s = utils.get_Sensors_fullInfo_by_username(username)
            #s = utils.get_Sensors_by_username(username)
            s = utils.get_Sensors_fullInfo_by_username(username)
        else:
            s = utils.get_Sensors_by_username(username)

        if s!=None:
            sensor_list = s
    else:
    	jsonify({"error": "No username specified in the parameter list"}), 400
    # Response to the GET /api/data     
    return jsonify(sensor_list), 200
        
        
if __name__ == '__main__':
    app.run(host='localhost', port=5010)
