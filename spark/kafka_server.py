import producer_server

BOOTSTRAP_SERVER = "PLAINTEXT://localhost:18101"
TOPIC_NAME = "com.udacity.streaming.sfpd"
INPUT_FILE = "./police-department-calls-for-service.json"

def run_kafka_server():

    producer = producer_server.ProducerServer(
        input_file=INPUT_FILE,
        topic=TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVER,
        client_id="sfpd.producer"
    )

    return producer

def feed():
    producer = run_kafka_server()
    producer.generate_data()

if __name__ == "__main__":
    feed()
