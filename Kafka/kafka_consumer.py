from kafka import KafkaConsumer
import json
import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Configure Kafka consumer
KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_URL)

# Function to upload a file to Google Cloud Storage
def upload_file_to_google_cloud_storage(bucket_name, file_name, local_path):
    client = storage.Client.from_service_account_json('service-account\key.json')
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(local_path, content_type='application/json')

    print(f'File {local_path} uploaded to {file_name} in {bucket_name} bucket.')

# Processing messages from Kafka
for message in consumer:
    values = message.value.decode('utf-8')
    print("Received data:", values)
    
    # Assuming the received data is JSON
    json_data = json.loads(values)
    
    # Write JSON data to a local file
    local_file_path = 'output_received.json'
    with open(local_file_path, 'w') as local_file:
        json.dump(json_data, local_file)
    
    # Upload the local file to Google Cloud Storage
    upload_file_to_google_cloud_storage('it4043e-it5384', 'it4043e/it4043e_group11_problem2/output.json', 'output.json')
