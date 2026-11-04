#!/usr/bin/env python
import os, sys, threading, time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', '127.0.0.1:9092')


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

        while not self.stop_event.is_set():
            producer.send('my-topic', b"test")
            producer.send('my-topic', b"\xc2Hola, mundo!")
            time.sleep(1)

        producer.close()


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS,
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['my-topic'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    # Create 'my-topic' Kafka topic
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

        topic = NewTopic(name='my-topic',
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"Could not create my-topic on {BOOTSTRAP_SERVERS}: {e!r}", file=sys.stderr)
        sys.exit(3)

    tasks = [
        Producer(),
        Consumer()
    ]

    # Start threads of a publisher/producer and a subscriber/consumer to 'my-topic' Kafka topic
    for t in tasks:
        t.start()

    time.sleep(10)

    # Stop threads
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()
