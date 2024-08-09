import sys, os
sys.path.append(os.path.abspath('/VOL200GB/framework/kg'))
from KG import Session

def get_MQTT_KAFKA_Streams():
	q = """SELECT ?mqtt_topic ?kafka_topic
	        WHERE {?mqtt_s a dg:MQTTStream;
   		              dg:topic ?mqtt_topic.
   	              ?kafka_s dg:topic ?kafka_topic;
   	            	       prov:wasGeneratedBy ?mqtt_s. 	   
    	             }
	"""	
	session = Session()
	result = session.query(q)
	return [{"mqtt_topic":x["mqtt_topic"]["value"],"kafka_topic":x["kafka_topic"]["value"]} for x in result]
	
	
	
def get_Sensor_Info(dbms):
    sensor_info = []
    for sen in __get_Sensor_Metadata(dbms):
        tags = __get_Tags(sen["kafka_stream"])
        sen["tags"] = tags
        fields = __get_Fields_to_store(sen["kafka_stream"])
        sen["fields_to_store"] = fields
        del sen["kafka_stream"]
        sensor_info.append(sen)
    	
    return sensor_info 


def __get_Sensor_Metadata(dbms):
	q = """SELECT ?sensor ?database ?table ?topic ?kafka_stream
   WHERE {?kafka_stream a dg:KafkaStream;
   			prov:wasAttributedTo ?sensor;
   			dg:topic ?topic;
   			dg:hasStorageSpecs ?spec.
     ?spec dg:dbms \""""+dbms+"""\";
           dg:database ?database;
   	       dg:table ?table.
}
	"""	
	session = Session()
	result = session.query(q)
	sensor_information = [{"sensor_id":x["sensor"]["value"],
	"database":x["database"]["value"],
	"table":x["table"]["value"],
	"topic":x["topic"]["value"],
	"kafka_stream":x["kafka_stream"]["value"]} for x in result]
	return sensor_information
	
def __get_Tags(kafka_stream):
	q = f"""SELECT ?tag
		WHERE {{ <{kafka_stream}> dg:hasStorageSpecs ?spec.
		?spec dg:tag ?tag}}
	"""	
	session = Session()
	result = session.query(q)
	return [x["tag"]["value"] for x in result]


def __get_Fields_to_store(kafka_stream):
	q = f"""SELECT ?field
		WHERE {{ <{kafka_stream}> dg:hasStorageSpecs ?spec.
	       ?spec dg:fieldToStore ?field}}
	"""
	session = Session()
	result = session.query(q)
	return [x["field"]["value"] for x in result]
