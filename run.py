from KafkaProducer import ProduceMessage
from Parser import ParseJson
from KafkaConsumer import ConsumeMessage
import sys
from datetime import datetime

if __name__ == '__main__':
    if str(sys.argv[1]) == "producer":
        parsedReadings = ParseJson().parser()
        ProduceMessage().producer(parsedReadings, sys.argv[2])
    if str(sys.argv[1]) == "consumer":
        ConsumeMessage().consumer()
