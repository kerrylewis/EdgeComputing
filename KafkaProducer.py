from kafka import KafkaProducer
import json
import time
import datetime


class ProduceMessage(object):

    def producer(self, data):
        # initialise the producer so that the broker can respond to the API request
        producer = KafkaProducer(bootstrap_servers='192.168.1.131:9092,192.168.1.132:9092,192.168.1.133:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
        # produce readings every 3 seconds
        for reading in data:
            reading["timestamp"] = str(datetime.now())
            producer.send('aq-data', reading)
            time.sleep(3)
            # prints the producer performance metrics
        producer.close(timeout=60)
