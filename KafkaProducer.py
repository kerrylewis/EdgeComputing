from kafka import KafkaProducer
import json
import time


class ProduceMessage(object):

    def producer(self, data):
        # initialise the producer so that the broker can respond to the API request
        producer = KafkaProducer(bootstrap_servers='192.168.1.130', value_serializer=lambda m: json.dumps(m).encode('ascii'))
        # produce readings every 3 seconds
        for reading in data:
            producer.send('aq-data', reading)
            time.sleep(3)
            # prints the producer performance metrics
            print(producer.metrics())
        producer.close(timeout=60)
