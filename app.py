from flask import Flask
import configparser
import pyodbc
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

app = Flask(__name__)

def create_kafka_producer():
    config_parser = configparser.ConfigParser()
    config_parser.read('kafka_config.ini')
    config = dict(config_parser['default'])
    producer = AvroProducer(config, default_key_schema=None, default_value_schema=None)
    return producer

def create_snowflake_connection():
    # Here you need to specify your connection details
    conn_str = (
        "DRIVER={SnowflakeDSIIDriver};"
        "Server=your_snowflake_account.snowflakecomputing.com;"
        "Database=your_database;"
        "Warehouse=your_warehouse;"
        "Schema=your_schema;"
        "UID=your_username;"
        "PWD=your_password;"
        "Role=your_role;"
        # Add other parameters as needed
    )
    connection = pyodbc.connect(conn_str, autocommit=True)
    return connection

def fetch_data_from_snowflake():
    conn = create_snowflake_connection()
    cursor = conn.cursor()

    # The query now selects all data from the view
    query = "SELECT * FROM your_view_name"
    
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Convert the data into a list of dictionaries so it can be easily serialized to JSON
    columns = [column[0] for column in cursor.description]
    results = []
    for row in rows:
        record = dict(zip(columns, row))
        results.append(record)
    return results

def send_to_kafka(producer, topic, data):
    for record in data:
        # Serialize our record to JSON
        message = json.dumps(record).encode('utf-8')
        producer.produce(topic=topic, key=None, value=message)
    producer.flush()

@app.route('/send')
def send_data():
    producer = create_kafka_producer()
    topic = "your_kafka_topic_name"
    
    data = fetch_data_from_snowflake()
    if data:
        send_to_kafka(producer, topic, data)
        return "Data sent to Kafka", 200
    else:
        return "No data to send", 200

if __name__ == "__main__":
    app.run(debug=True)
