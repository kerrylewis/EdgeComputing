from kafka import KafkaConsumer
import json
from datetime import datetime


class ConsumeMessage(object):

    def consumer(self):
        # sets the topic of consumer to air quality data
        consumer = KafkaConsumer(bootstrap_servers='192.168.1.131:9092,192.168.1.132:9092,192.168.1.133:9092')
        consumer.subscribe('aq-data')
        # print the consumed messages and return array of data
        consumedData = []
        for msg in consumer:
           receiveTime = datetime.now()
           record = json.loads(msg.value.decode("utf-8"))
           #print(str(msg[6]["timestamp"]))
           sendTime = datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S.%f")

           latency = receiveTime - sendTime
           consumedData.append(latency);

           if(len(consumedData) == 100):
               avgLatency = None
               for latency in consumedData:
                   if avgLatency is None:
                       avgLatency = latency
                   else:
                       avgLatency = avgLatency + latency
               result = avgLatency / 100
               print(result)
           #print("Latency:" + str(latency))
        return consumedData
        # print consumer metrics
