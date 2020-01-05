from confluent_kafka import Consumer, KafkaException
import sys

BOOTSTRAP_SERVER = "PLAINTEXT://localhost:18101"
TOPIC_NAME = "com.udacity.streaming.sfpd"

def consume():
    conf = {'bootstrap.servers': BOOTSTRAP_SERVER, 'group.id': "testConsumer", 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'}

    c = Consumer(**conf)
    c.subscribe([TOPIC_NAME])

    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))
            print(msg.value())

if __name__ == "__main__":
    consume()

