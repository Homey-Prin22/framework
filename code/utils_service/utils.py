import sys, os
sys.path.append(os.path.abspath('/framework/code/kg'))
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
    sensor_information = [{"source_id":x["sensor"]["value"],
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
    sensor_information = [{"source_id":x["sensor"]["value"],
	"topic":x["topic"]["value"],
	"kafka_stream":x["kafka_stream"]["value"]} for x in result]
    
    sensor_info = []
    for sen in sensor_information:
        fields = __get_Fields_to_monitor(sen["kafka_stream"])
        sen["fields_to_monitor"] = fields
        del sen["kafka_stream"]
        sensor_info.append(sen)
    return sensor_info
	

def get_Role_by_username(username):#da testare
    q = f"""SELECT ?role 
            WHERE {{ ?a rdfs:label <{username}>.
                     ?m org:membership ?a.
                     ?m org:role ?role.            
              }}
    """
    session = Session()
    result = session.query(q)
    role_list = [x["role"]["value"] for x in result]
	
    return role_list
    
    
def __get_Fields_to_monitor(kafka_stream):
	q = f"""SELECT ?field
		WHERE {{ <{kafka_stream}> dg:hasMonitoringSpecs ?spec.
	       ?spec dg:fieldToMonitor ?field}}
	"""
	session = Session()
	result = session.query(q)
	return [x["field"]["value"] for x in result]
	
	
def get_Sensors_by_username(username):
	q = f"""SELECT distinct ?s  ?topic ?so ?site
   	WHERE {{?s a ioe:Sensor;
            ioe:includedIn ?so.
            ?so a ioe:SmartObject;
            ioe:locatedIn ?site.
            ?parent_site bot:containsZone ?site.
    
        ?kafka_stream a dg:KafkaStream;
   		prov:wasAttributedTo ?s;
   		dg:topic ?topic.
    
        ?m org:member ?agent;
            org:role ?r.
	   ?agent foaf:account ?account. 
           ?ri ioe:forRole ?r.
        ?ri ?p ?o
         FILTER ((?account = "{username}") && ((?p = ioe:onSystem && ?o = ?s) ||
                 (?p = ioe:onSmartObject && ?o = ?so) ||
            (?p = ioe:onEnvironment && (?o = ?site || ?o = ?parent_site)))) 
        }}
      	ORDER BY ?site ?so
	"""
	print(q)
	session = Session()
	result = session.query(q)
	sensor_list = [{"source_id":x["s"]["value"], 
	  "topic":x["topic"]["value"],
	  "smart_object":x["so"]["value"],
	  "site":x["site"]["value"]} for x in result]
	return sensor_list
	
	
def get_Sensors_schema(source_id,username):
	q = f"""SELECT ?fieldPath ?property ?fieldDescription ?uom_symbol ?uom_label
   	WHERE {{?s a ioe:Sensor;
            ioe:includedIn ?so.
            ?so a ioe:SmartObject;
            ioe:locatedIn ?site.
            ?parent_site bot:containsZone ?site.
    
        ?kafka_stream a dg:KafkaStream;
   		prov:wasAttributedTo ?s;
   		dg:topic ?topic;
   		dg:hasSchema ?schema.
   	?schema dg:hasField ?field.
	
   	?field a dg:Field;
   		dg:relatedProperty ?property;
   		dg:fieldPath ?fieldPath.
   	?property rdfs:comment ?fieldDescription;
   		qudt:unit ?uom.
   	
   	?uom qudt:symbol ?uom_symbol;
   	     rdfs:label ?uom_label.
    
   
        ?m org:member ?agent;
            org:role ?r.
	   ?agent foaf:account ?account. 
           ?ri ioe:forRole ?r.
        ?ri ?p ?o
         FILTER ((?account = "{username}") && (?s = <{source_id}>) && ((?p = ioe:onSystem && ?o = ?s) ||
                 (?p = ioe:onSmartObject && ?o = ?so) ||
            (?p = ioe:onEnvironment && (?o = ?site || ?o = ?parent_site)))) 
        }}
      	ORDER BY ?site ?so
	"""
	#print(q)
	session = Session()
	result = session.query(q)
	sensor_schema = [{"fieldPath":x["fieldPath"]["value"], 
	  "property":x["property"]["value"],
	  "fieldDescription":x["fieldDescription"]["value"],
	  "uom_label":x["uom_label"]["value"],
	  "uom_symbol":x["uom_symbol"]["value"],} for x in result]
	  
	return sensor_schema
	
def get_Sensors_fullInfo_by_username(username):
	q = f"""SELECT distinct ?s  ?topic ?so ?site ?fields
   	WHERE {{?s a ioe:Sensor;
            ioe:includedIn ?so.
            ?so a ioe:SmartObject;
            ioe:locatedIn ?site.
            ?parent_site bot:containsZone ?site.
    
        ?kafka_stream a dg:KafkaStream;
   		prov:wasAttributedTo ?s;
   		dg:topic ?topic.
    
        ?m org:member ?agent;
            org:role ?r.
	   ?agent foaf:account ?account. 
           ?ri ioe:forRole ?r.
        ?ri ?p ?o
         FILTER ((?account = "{username}") && ((?p = ioe:onSystem && ?o = ?s) ||
                 (?p = ioe:onSmartObject && ?o = ?so) ||
            (?p = ioe:onEnvironment && (?o = ?site || ?o = ?parent_site)))) 
        }}
      	ORDER BY ?site ?so
	"""
	print(q)
	session = Session()
	result = session.query(q)
	sensor_list = [{"source_id":x["s"]["value"], 
	  "topic":x["topic"]["value"],
	  "smart_object":x["so"]["value"],
	  "site":x["site"]["value"],
	  "fields":get_Sensors_schema(x["s"]["value"],username)
	  } for x in result]
	return sensor_list
