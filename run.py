from KafkaProducer import ProduceMessage
from Parser import ParseJson
from KafkaConsumer import ConsumeMessage
import sys
from datetime import datetime

if __name__ == '__main__':

    now = str(datetime.now())
    print(type(now))
    print("now as string: " + now)
    nowDate = datetime.strptime(now, "%Y-%m-%d %H:%M:%S.%f")
    print(type(nowDate))
    if str(sys.argv[1]) == "producer":
        parsedReadings = ParseJson().parser()
        ProduceMessage().producer(parsedReadings)
    if str(sys.argv[1]) == "consumer":
        ConsumeMessage().consumer()
