import sys
import traceback
import contextlib
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

c = AvroConsumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'group1',
    'schema.registry.url': 'http://schemaregistry:8081',
})
topic = 'page_visits'
c.subscribe([topic])

print("Starting to consume kafka topic {t}".format(t=topic))

with contextlib.closing(c):
    while True:
        msg = c.poll(10)
        if msg:
            if not msg.error():
                print(msg.value())
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                break

