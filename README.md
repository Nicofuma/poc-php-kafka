PHP - Kafka - AVRO POC
======================

POC the usage of Kafka with AVRO messages and the schema registry to manage the schemas.

Usage
-----

`docker-compose up -d` To start kafka and zookeeper
`docker-compose exec kafka kafka-topics.sh --create --zookeeper zookeeper --replication-factor 1 --partitions 5 --topic members`
`docker-compose run --rm php php src/produce.php <nb messages>` To produce one or more messages
`docker-compose run --rm php php src/consume.php` To launch a consumer
`docker-compose down --remove-orphans -v` To stop everything

Note: All consumers are created in the same group and consume the same topic. It means that a message won't be consumed twice by these consumers.
Note: As the topic is created with 5 partitions, you can't handle more than 5 messages at the same time.

Issues
------

The consumers group feature only works with librdkafka 0.9+ and currently php-rdkafka is incompatible with librdkafka 0.9.2 and librdkafka 0.9.3. You need to force the librdkafka version to 0.9.1 when installing it.

Note
----

librdkafka and php-rdkafka seems both to be under active development and pretty up-to-date with current kafka features
The AVRO library does not seems to be well maintained but the project does not evolve that much so it does not seems to an issue.
