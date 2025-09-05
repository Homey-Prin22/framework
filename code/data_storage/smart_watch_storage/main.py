import json
from data_flatten_router import align_payload_by_type
from influxdb_client import Point, WritePrecision, WriteOptions
from kafka_consumer import KafkaEc
from kafka_consumer import kafka_consumer
from db_utils import connection_to_influxdb

KAFKA_TOPIC = "smart_watch_acquisition"

def process_message(message_json: str, client):
    try:
        # json.loads per eliminare i caratteri di escape \
        outer = json.loads(message_json)
        # json.loads per estrarre il contenuto all'interno della chiave 'data'
        #inner = json.loads(outer["data"])
        inner = json.loads(message_json["data"])
        # per estarre le informazioni nel corpo di identifier: token, id, device, model. poi c'è il campo sensorData
        identifier = inner.get("identifier", {})
        # per estrarre il payload ovvero contenuto della chiave sensorData
        payload = {k: v for k, v in inner.items() if k != "identifier"}
        print(payload)

        aligned_data = align_payload_by_type(payload)

        for entry in aligned_data:
            timestamp = entry.get("timestamp")
            point = Point("sensor_data") \
                .tag("device_id", identifier.get("id")) \
                .tag("device", identifier.get("device", "unknown")) \
                .tag("model", identifier.get("model", "unknown"))

            for k, v in entry.items():
                #if k != "timestamp" and isinstance(v, (int, float)):
                if isinstance(v, (int, float)):
                    point.field(k, v)
            try:
                write_api = client.write_api(write_options=WriteOptions(batch_size=1))
                bucket = "smart_watch_acquisition"
                org = "HOMEY"
                write_api.write(bucket=bucket, org=org, record=point, write_precision=WritePrecision.NS)
            except Exception as e:
                print(f"error: {e}")
            print(f"Data written in InfluxDB")

            write_api.flush()

    except Exception as e:
        print(f"Error while processing message: {e}")

def main():
    consumer = kafka_consumer(KAFKA_TOPIC)
    client = connection_to_influxdb()

    try:
        for message in consumer:
            print(message)
            print(message.value)
            #process_message(message.value, influxdb_client, sensor_info)
            if message is None:
                continue
            else:
                #message_value = message.value().decode("utf-8")
                process_message(message, client)
    except KeyboardInterrupt:
        print("Interrupted")

if __name__ == "__main__":
    main()