from KafkaProducer import ProduceMessage
from Parser import ParseJson
from KafkaConsumer import ConsumeMessage
import sys

if __name__ == '__main__':
    if str(sys.argv[1]) == "producer":
        parsedReadings = ParseJson().parser()
        ProduceMessage().producer(parsedReadings)
    if str(sys.argv[1]) == "consumer":
        ConsumeMessage().consumer()
