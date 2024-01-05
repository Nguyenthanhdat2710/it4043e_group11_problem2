from typing import List, Union
from kafka import KafkaProducer
import os
import json
import sys
import yaml
from dotenv import load_dotenv

load_dotenv()

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

# Function to read YAML configuration
def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read YAML config successfully")
    return config

KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Configure Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_URL, value_serializer=lambda K: json.dumps(K).encode('utf-8'))

# Specify the JSON filename
json_filename = 'output.json'

def convert_to_json(data, json_filename=json_filename):
    if data is not None:
        data = [i for n, i in enumerate(data) if i not in data[:n]]
        with open(os.path.join("data", json_filename), 'w', encoding='utf-8') as json_file:
            json_file.write('[')
            for idx, tweet in enumerate(data):
                json.dump(tweet, json_file, ensure_ascii=False, indent=4, default=str)
                if idx < len(data) - 1:
                    json_file.write(',')
                json_file.write('\n')
            json_file.write(']')
    else:
        print("Error: 'data' is None.")

# Function to crawl tweets of users
def crawl_tweet_user(app,
                     users: Union[str, List[str]],
                     username: Union[str, List[str]],
                     pages: int = 10,
                     wait_time: int = 5):
    for idx, user in enumerate(users):
        print(f"Crawling tweets of '@{user}'")
        all_tweets = app.get_tweets(username=f"{user}", pages=pages, wait_time=wait_time)
        if all_tweets is not None:
            convert_to_json(all_tweets, f"{username[idx]}.json")
            with open(os.path.join("data", f"{username[idx]}.json")) as f:
                data = json.load(f)
                producer.send(KAFKA_TOPIC, value=data)

# Main script
if __name__ == "__main__":
    # Replace with your custom Twitter module and credentials
    app = MyTwitter("session")
    with open("account.key", "r") as f:
        username, password, key = f.read().split()
    app.sign_in(username, password, extra=key)

    # Read config file
    CONFIG_PATH = os.path.join(os.getcwd(), "config_users.yaml")
    config = read_yaml(path=CONFIG_PATH)

    # Crawl tweets and send to Kafka
    crawl_tweet_user(
        app=app,
        users=config['users'],
        username=config['username'],
        pages=config['pages'],
        wait_time=config['wait_time']
    )

    # Close Kafka producer
    producer.close()
