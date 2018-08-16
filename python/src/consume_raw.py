import contextlib

from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'groupraw',
})
topic = 'page_visits'
c.subscribe([topic])

print("Starting to consume kafka topic {t!r}".format(t=topic))

with contextlib.closing(c):
    while True:
        msg = c.poll(1)
        if msg:
            print((
                repr(msg.key()),
                repr(msg.value()),
                msg.error()
            ))
