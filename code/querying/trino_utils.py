import trino
import json
from trino.exceptions import TrinoConnectionError, TrinoQueryError
from datetime import datetime

def connect_to_trino():
    try:
        conn = trino.dbapi.connect(
            host='localhost',
            port=7070,
            user='admin',
            #http_scheme='http',
            #verify=True
        )
        print("Connection to Trino established.")
        return conn

    except TrinoConnectionError as e:
        print(f"Error during connection to Trino: {e}")
    return None

def run_query(conn, final_query):

    try:
        cursor = conn.cursor()
        cursor.execute(final_query)
        result = cursor.fetchall()
        
        column_names = [desc[0] for desc in cursor.description]
        if result:
            formatted_rows = [
                {column_names[i]: (field.isoformat() if isinstance(field, datetime) else field) for i, field in enumerate(row)}
                for row in result
            ]
            print("Query Results:")
            for row in formatted_rows:
                print(row)
            return json.dumps(formatted_rows, default=str, indent=4)

        else:
            print("No data found for the given parameter.")
            return json.dumps({"message": "No data found for the given parameters."})
    except Exception as e:
        print("Query exception:", e)
        return json.dumps({"error": f"Query execution error: {str(e)}"})
    finally:
        cursor.close()
        conn.close()