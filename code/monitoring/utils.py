import sys, os
sys.path.append(os.path.abspath('/VOL200GB/framework/kg'))
from KG import Session


def get_Sensor_Info_by_IDs(sensor_id):
    q = """SELECT ?sensor ?topic ?kafka_stream
   WHERE {?kafka_stream a dg:KafkaStream;
   			prov:wasAttributedTo ?sensor;
   			dg:topic ?topic.
	"""	
    #
    sensor_id_to_string = '>, <'.join(sensor_id)
    q = q + "\nFILTER (?sensor IN (<" + sensor_id_to_string + ">)).}"
    session = Session()
    result = session.query(q)
    sensor_information = [{"sensor_id":x["sensor"]["value"],
	"topic":x["topic"]["value"],
	"kafka_stream":x["kafka_stream"]["value"]} for x in result]
    
    sensor_info = []
    for sen in sensor_information:
        fields = __get_Fields_to_monitor(sen["kafka_stream"])
        sen["fields_to_monitor"] = fields
        del sen["kafka_stream"]
        sensor_info.append(sen)
    return sensor_info
	
    	
def get_Sensor_Info_by_filter(site,measure):
    q = """SELECT ?sensor ?topic ?kafka_stream
   WHERE {?kafka_stream a dg:KafkaStream;
   			prov:wasAttributedTo ?sensor;
   			dg:topic ?topic.
	"""	
    #
    if len(site)>0:
        sites_to_string = '>, <'.join(site)
        q = q + """?sensor ioe:includedIn ?so. 
                   ?so ioe:locatedIn ?site.
                   OPTIONAL {?supersite bot:containsZone ?site}
                   """
        q = q + "\nFILTER (?site IN (<" + sites_to_string + ">) || ?supersite IN (<" + sites_to_string + ">))."
    
    if len(measure)>0:
        measures_to_string = '>, <'.join(measure)
        q = q + "?sensor sosa:observes ?p."
        q = q + "\nFILTER (?p IN (<" + measures_to_string + ">))."
    
    
    q = q + "}"
    
    session = Session()
    result = session.query(q)
    sensor_information = [{"sensor_id":x["sensor"]["value"],
	"topic":x["topic"]["value"],
	"kafka_stream":x["kafka_stream"]["value"]} for x in result]
    
    sensor_info = []
    for sen in sensor_information:
        fields = __get_Fields_to_monitor(sen["kafka_stream"])
        sen["fields_to_monitor"] = fields
        del sen["kafka_stream"]
        sensor_info.append(sen)
    return sensor_info
	


def __get_Fields_to_monitor(kafka_stream):
	q = f"""SELECT ?field
		WHERE {{ <{kafka_stream}> dg:hasMonitoringSpecs ?spec.
	       ?spec dg:fieldToMonitor ?field}}
	"""
	session = Session()
	result = session.query(q)
	return [x["field"]["value"] for x in result]
