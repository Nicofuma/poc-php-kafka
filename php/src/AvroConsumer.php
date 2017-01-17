<?php

use RdKafka\Conf;
use RdKafka\KafkaConsumer;

/**
 * Kafka Consumer client which does avro schema decoding of messages.
 * Handles message deserialization.
 */
class AvroConsumer extends KafkaConsumer
{
    /** @var MessageSerializer */
    private $serializer;

    public function __construct(Conf $conf, $registryUrl, $options = [])
    {
        parent::__construct($conf);

        $this->serializer = new MessageSerializer(new CachedSchemaRegistryClient($registryUrl), $options);

    }

    /**
     * This is an overriden method from confluent_kafka.Consumer class. This handles message
     * deserialization using avro schema
     *
     * @param int $timeout
     *
     * @return array message object with deserialized key and value as array
     */
    public function consume($timeout)
    {
        $message = parent::consume($timeout);

        if (RD_KAFKA_RESP_ERR_NO_ERROR === $message->err) {
            if ($message->payload) {
                $message->payload = $this->serializer->decodeMessage($message->payload);
            }

            if ($message->key) {
                $message->key = $this->serializer->decodeMessage($message->key);
            }
        }

        return $message;
    }
}
