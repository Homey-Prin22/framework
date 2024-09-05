from rdflib import Namespace
from SPARQLWrapper import SPARQLWrapper, JSON

# Definition of standard namespaces
STANDARD_NAMESPACES = {
    'rdf': Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    'rdfs': Namespace("http://www.w3.org/2000/01/rdf-schema#"),
    'owl': Namespace("http://www.w3.org/2002/07/owl#"),
    'skos': Namespace("http://www.w3.org/2004/02/skos/core#"),
    'prov': Namespace("https://www.w3.org/TR/prov-o/#"),
    'bot': Namespace("https://w3id.org/bot#"), 
    'ioe': Namespace("http://w3id.org/semioe#"), 
    'kg': Namespace("http://example_graph/"), 
    'org': Namespace("http://www.w3.org/ns/org#"), 
    'ssn': Namespace("http://www.w3.org/ns/ssn/"), 
    'ssn-system': Namespace("http://www.w3.org/ns/ssn/systems/"), 
    'sosa': Namespace("http://www.w3.org/ns/sosa/"),
    'xsd': Namespace("http://www.w3.org/2001/XMLSchema#"), 
    'ex_kg': Namespace("http://homey/example_graph/"),
    'dg': Namespace("http://homey/data_gathering/")	    
}

DEFAULT_ENDPOINT = "http://192.168.104.78:7200/repositories/homey"

class Session:
    def __init__(self, namespaces=None, endpoint = DEFAULT_ENDPOINT):
        self.namespaces = self._merge_namespaces(namespaces)
        self.string_namespaces = self._namespaceToString()
        self.endpoint = endpoint
        self.sparql = SPARQLWrapper(endpoint)


    def _merge_namespaces(self, additional_namespaces):
        return {**STANDARD_NAMESPACES, **(additional_namespaces or {})}

    def _namespaceToString(self):
        items = ["PREFIX "+k+":"+" <"+v+">" for k,v in self.namespaces.items()]
        return "\n".join(items)
        
    def get_all(self,concept):
    	query = f"""SELECT ?s 
    	WHERE {{ ?s a <{concept}>}} LIMIT 100"""
    	
    	response = self.query(query)
    	return [item['s'].value for item in response]
    	
    def query(self, query):
        self.sparql.setReturnFormat(JSON)
        query = self.string_namespaces+"\n"+query
        self.sparql.setQuery(query)
        return self.sparql.queryAndConvert()["results"]["bindings"]

        	
