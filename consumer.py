from confluent_kafka import Consumer, KafkaError

def consume_messages(consumer, topic):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                continue
            else:
                print(msg.error())
                break

        print('Received message: {}'.format(msg.value().decode('utf-8')))

    consumer.close()

def main():
    bootstrap_servers = 'localhost:9092'
    topic = 'test-topic'

    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)

    consume_messages(consumer, topic)

if __name__ == '__main__':
    main()
