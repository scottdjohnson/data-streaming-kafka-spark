from kafka import KafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import json
import time

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.udacity.spark"

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        with open(self.input_file) as f:
            for line in f:
                message = self.dict_to_binary(line)
                self.send(self.topic, message)
                time.sleep(1)

    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')


def generate_data(self):
    with open(self.input_file) as f:
        for line in f:
            message = dict_to_binary(line)
            self.send(self.topic, message)
            time.sleep(1)

def dict_to_binary(json_dict):
    return json.dumps(json_dict).encode('utf-8')
    
def produce_sync(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    while True:
        start = time.time()*1000
        with open('./data/uber.json') as json_file:
            data = json.load(json_file)
            for d in data:
                print("data")
                print(d)
                p.produce(topic_name, dict_to_binary(d))
                p.flush()
                time.sleep(1)
    
def create_topic(client):
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            pass

def main():
    print("create")
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        print("produce")
        produce_sync(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")
        
if __name__ == "__main__":
    main()