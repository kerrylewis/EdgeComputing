from kafka import KafkaConsumer
import json


class ConsumeMessage(object):

    def consumer(self):
        # sets the topic of consumer to air quality data
        consumer = KafkaConsumer(bootstrap_servers='192.168.1.133')
        consumer.subscribe('aq-data')
        # print the consumed messages and return array of data
        consumedData = []
        for msg in consumer:
            consumedData.append(json.loads(msg.value.decode("utf-8")))
        print(consumer.metrics())
        return consumedData
        # print consumer metrics
