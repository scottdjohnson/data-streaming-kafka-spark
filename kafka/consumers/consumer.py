"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""
    BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.broker_properties = {
            "bootstrap.servers": self.BROKER_URL
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(
                {"bootstrap.servers": self.BROKER_URL, "group.id": "0"},
                schema_registry="http://localhost:8081"
            )
        else:
            self.consumer = Consumer(
                {
                    "bootstrap.servers": self.BROKER_URL,
                    "group.id": "0",
                    "auto.offset.reset":"earliest"
                }
            )
            pass

        self.consumer.subscribe( [self.topic_name_pattern], on_assign=self.on_assign )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
            pass
            #
            #
            # TODO
            #
            #

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        message = self.consumer.poll(1.0)
        if message is None:
            print("no message received by consumer")
            return 0
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
            return 0
        else:
            print(f"consumed message {message.key()}: {message.value()}")
            return 1


    def close(self):
        """Cleans up any open kafka consumers"""
