from kafka import KafkaConsumer
import json
import datetime


class ConsumeMessage(object):

    def consumer(self):
        # sets the topic of consumer to air quality data
        consumer = KafkaConsumer(bootstrap_servers='192.168.1.131:9092,192.168.1.132:9092,192.168.1.133:9092')
        consumer.subscribe('aq-data')
        # print the consumed messages and return array of data
        consumedData = []
        for msg in consumer:
           print(json.loads(msg.value.decode("utf-8")))
           sendTime = datetime.strptime(msg["timestamp"])
           receiveTime = datetime.datetime.now()
           latency = receiveTime - sendTime
           print("Latency:" + latency)
        return consumedData
        # print consumer metrics
