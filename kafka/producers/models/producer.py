"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
    SCHEMA_REGISTRY_URL = "http://localhost:8081"

    # Add schema registry???
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        # SCOTT: this is probably unused ???
        self.broker_properties = {
            "bootstrap.servers": self.BROKER_URL,
            "schema.registry.url": self.SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        # With schema registry ???
        self.producer = AvroProducer(
            {"bootstrap.servers": self.BROKER_URL,
            'schema.registry.url': self.SCHEMA_REGISTRY_URL},
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        ) #???

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        clusterMetadata = self.client.list_topics(timeout=60)
        if (clusterMetadata.topics.get(self.topic_name) is None):
            logger.info(f"Creating topic {self.topic_name}")
            """Creates the topic with the given topic name"""
            futures = self.client.create_topics(
                [
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=1,#self.num_partitions,
                        replication_factor=1, #self.num_replicas,
                        config={
                            "delete.retention.ms":100,
                            "compression.type":"lz4",
                            "file.delete.delay.ms":100
                        }

                    )
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info("topic created")
                except Exception as e:
                    logger.error(f"failed to create topic {self.topic_name}: {e}")
                    raise

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        future = self.client.delete_topics(list(self.topic_name), operation_timeout=30)

        # Wait for operation to finish.
        for topic, f in future.items():
            try:
                f.result()  # The result itself is None
                logger.info("Topic {} deleted".format(topic))
            except Exception as e:
                logger.error("Failed to delete topic {}: {}".format(topic, e))

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
