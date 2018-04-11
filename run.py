from KafkaProducer import ProduceMessage
from Parser import ParseJson
from KafkaConsumer import ConsumeMessage

if __name__ == '__main__':
    parsedReadings = ParseJson().parser()
    ProduceMessage().producer(parsedReadings)
    ConsumeMessage().consumer()
