#from kafka import KafkaProducer, KafkaConsumer

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

import json
import time


class ProducerServer(Producer):

    def __init__(self, input_file, topic, bootstrap_servers, client_id):
        conf = {'bootstrap.servers': bootstrap_servers}
        super().__init__(**conf)
        self.input_file = input_file
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id

    def generate_data(self):
        self.create_topic()
        with open(self.input_file) as json_file:
            data = json.load(json_file)
            for d in data:
                print(f"Producing: {d}")
                self.produce(self.topic, self.dict_to_binary(d))
                self.flush()
                time.sleep(1)

    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')

    def create_topic(self):
        """Creates the topic with the given topic name"""
        conf = {'bootstrap.servers': self.bootstrap_servers, 'group.id': 'listTopics', 'session.timeout.ms': 6000,
                'auto.offset.reset': 'latest'}
        consumer = Consumer(**conf)
        topics = consumer.list_topics().topics.keys()
        if (self.topic not in topics):
            client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            futures = client.create_topics([NewTopic(topic=self.topic, num_partitions=1, replication_factor=1)])

            for _, future in futures.items():
                try:
                    future.result()
                except Exception as e:
                    pass
