from confluent_kafka import Producer
import sys

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_message(producer, topic, message):
    producer.produce(topic, value=message, callback=delivery_report)
    producer.poll(0)

def main():
    bootstrap_servers = 'localhost:9092'
    topic = 'test-topic'
    message = 'Hello, Mano!'

    producer_conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_conf)

    produce_message(producer, topic, message)

    producer.flush()

if __name__ == '__main__':
    main()
