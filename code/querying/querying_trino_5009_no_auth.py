import json
import time
from datetime import datetime, timedelta
from flask import Flask, request, Response, jsonify
from trino_utils import run_query, connect_to_trino
from api_data_utils import get_sensor_list

app = Flask(__name__)

def convert_timestamp(timestamp):
    try:
        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        timestamp_ms = int(dt.timestamp() * 1000)
        return timestamp_ms
    except ValueError:
        return None

def sensor_list():
    with open("/querying/output.json", "r") as file:
        data = json.load(file)
    sensor_list = []
    for key, value in data.items():
        sensor_list.append(key)
    return sensor_list

def allowed_param_list():
    with open("/querying/output.json", "r") as file:
        data = json.load(file)
    source_ids_list = get_sensor_list('john.smith', 'source_id')

    param_list = []

    for key, value in data.items():
        if (key in source_ids_list):
            for k, v in value.items():
                if (k not in ['sensor_id', 'db', 'timestamp']) and (k not in param_list):
                    param_list.append(k)
    return param_list


def build_param_query(source_id, param, db, sensor_id, timestamp, aggr, limit, offset, start_time=None, stop_time=None):

    base_query = f"(SELECT '{source_id}' AS source_id, {sensor_id}, "
    
    if aggr:
        base_query += f"{aggr}({param}) AS {aggr}_{param} "
    else:
        base_query += f"{param}, {timestamp}"

    base_query += f" FROM {db}"
    
    conditions = []
    if start_time:
        conditions.append(f"{timestamp} > {start_time}")
    if stop_time:
        conditions.append(f"{timestamp} < {stop_time}")
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    if aggr:
        base_query += f" GROUP BY {sensor_id}"
    else:
        base_query += f" ORDER BY {timestamp} ASC"

    if limit:
        #offset = (page - 1) * limit
        base_query += f" OFFSET {offset} LIMIT {limit}"

    base_query += ")"

    return base_query


def build_sensor_query(source_id, param, db, sensor_id, timestamp, aggr, limit, offset, start_time=None, stop_time=None):
    
    base_query = f"(SELECT '{source_id}' AS source_id, {sensor_id}, "
    
    if param:
        if aggr:
            base_query += f"{aggr}({param}) AS {aggr}_{param} "
        else:
            base_query += f"{param}, {timestamp}"
    else:
        base_query += "*"

    base_query += f" FROM {db}"
    
    conditions = []
    if start_time:
        conditions.append(f"{timestamp} > {start_time}")
    if stop_time:
        conditions.append(f"{timestamp} < {stop_time}")
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    if aggr:
        base_query += f" GROUP BY {sensor_id}"
    else:
        base_query += f" ORDER BY {timestamp} ASC"

    if limit:
        #offset = (page - 1) * limit
        base_query += f" OFFSET {offset} LIMIT {limit}"
    base_query += ")"

    return base_query


def build_query(source_id, param, aggr="", limit="", offset="", start="", stop=""):

    # Output from KG
    with open("/querying/output.json", "r") as file:
        data = json.load(file)
    
    #source_ids_list = get_sensor_list('john.smith', 'source_id')

    # Extracts IDs of the sensors measuring that particular parameter from the JSON file and saves them in sensor_list2
    if param:
        sensor_list2 = []
        for key, value in data.items():
            if (param in value):
                    sensor_list2.append(key)
        if not sensor_list2:
            return None

    query_parts = []

    if source_id:
        if param:
            paramm = data[source_id][param]
        else:
            paramm = None
        final_query = build_sensor_query(
                source_id=source_id,
                param=paramm,
                db=data[source_id]["db"],
                sensor_id=data[source_id]["sensor_id"],
                timestamp=data[source_id]["timestamp"],
                aggr=aggr or None,
                limit=limit or None,
                offset=offset or 0,
                start_time=start or None,
                stop_time=stop or None,
            )
    else:
        # Use the build_select_query function to construct queries
        for sensor in sensor_list2:
            if sensor in data and param in data[sensor]:
                query = build_param_query(
                    source_id=sensor,
                    param=data[sensor][param],
                    db=data[sensor]["db"],
                    sensor_id=data[sensor]["sensor_id"],
                    timestamp=data[sensor]["timestamp"],
                    aggr=aggr or None,
                    limit=limit or None,
                    offset=offset or 0,
                    start_time=start or None,
                    stop_time=stop or None,
                )
                query_parts.append(query)

        # Assembles the final query, with the concatenation of the elements of the query_parts list via UNION
        final_query = " UNION ".join(query_parts)
    return final_query



# Endpoint to querying Trino database
@app.route('/query-trino', methods=['GET'])
def get_data():

    connection = connect_to_trino()
    if not connection:
        return Response(json.dumps({"error": "Connection to Trino failed"}), content_type='application/json', status=500)
    
    param = request.args.get("param")
    aggr = request.args.get("aggr")
    limit = request.args.get("limit")
    offset = request.args.get("offset")
    start_time = request.args.get("start_time")
    stop_time = request.args.get("stop_time")
    source_id = request.args.get("source_id")

    if not source_id and not param:
        return Response(json.dumps({"error": "Missing parameters. Please provide at least 'param' or 'source_id'."}), content_type='application/json', status=400)
    
    #valid_sensor_ids = sensor_list()
    source_ids_list = get_sensor_list('john.smith', 'source_id')
    if source_id and source_id not in source_ids_list:
        return Response(json.dumps({"error": f"Invalid 'source_id' value, must be one of {source_ids_list}"}), content_type='application/json', status=400)

    valid_params = ["temperature", "humidity", "pollution", "power", "surface_temperature", "engine_temperature", "noise_pollution", "velocity", "vibration"]
    if param and param not in valid_params:
        return Response(json.dumps({"error": f"Invalid 'param' value, must be one of {valid_params}"}), content_type='application/json', status=400)

    valid_aggr = ["avg", "min", "max"]
    if aggr and aggr not in valid_aggr:
        return Response(json.dumps({"error": f"Invalid aggregation form value, must be one of {valid_aggr}"}), content_type='application/json', status=400)

    if limit and not limit.isdigit():
        return Response(json.dumps({"error": "Invalid 'limit' value, must be numeric"}), content_type='application/json', status=400)
    
    if not start_time:
        start_time = '' #default
    else:
        start_time = convert_timestamp(start_time)
        if start_time is None:
            return Response(json.dumps({"error": "Invalid 'start_time' value: use the formato YYYY-MM-DD HH:MM:SS"}), content_type='application/json', status=400)
        
    if not stop_time:
        stop_time = '' #default
    else:
        stop_time = convert_timestamp(stop_time)
        if stop_time is None:
            return Response(json.dumps({"error": "Invalid 'stop_time' value: use the formato YYYY-MM-DD HH:MM:SS"}), content_type='application/json', status=400)

    try:
        final_query = build_query(source_id, param, aggr, limit, offset, start_time, stop_time)
        output = run_query(connection, final_query)
        return Response(output, content_type='application/json', status=200)
    except Exception as e:
        print("Query exception:", e)
        return Response({"error: Query exception"}, content_type='application/json', status=200)

    finally:
        connection.close()



if __name__ == '__main__':
    app.run(host='localhost', port=5009)