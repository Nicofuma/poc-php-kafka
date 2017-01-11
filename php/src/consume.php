<?php

require __DIR__.'/../vendor/autoload.php';

$schema = '';

$schema = <<<_JSON
{
    "name": "member",
    "type": "record",
    "fields":
    [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"}
    ]
}
_JSON
;

$schema = AvroSchema::parse($schema);

$conf = new \RdKafka\Conf();

// Set a rebalance callback to log partition assignments (optional)
$conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
    switch ($err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            echo "Assign: ";
            var_dump($partitions);
            $kafka->assign($partitions);
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            echo "Revoke: ";
            var_dump($partitions);
            $kafka->assign(NULL);
            break;

        default:
            throw new \Exception($err);
    }
});

// Configure the group.id. All consumer with the same group.id will consume
// different partitions.
$conf->set('group.id', 'group1');

// Initial list of Kafka brokers
$conf->set('metadata.broker.list', 'kafka:9092');

$topicConf = new \RdKafka\TopicConf();

// Set where to start consuming messages when there is no initial offset in
// offset store or the desired offset is out of range.
// 'smallest': start from the beginning
$topicConf->set('auto.offset.reset', 'smallest');

// Set the configuration to use for subscribed/assigned topics
$conf->setDefaultTopicConf($topicConf);

$consumer = new \RdKafka\KafkaConsumer($conf);

// Subscribe to topic 'test'
$consumer->subscribe(['members']);

echo "Waiting for partition assignment... (make take some time when\n";
echo "quickly re-joining the group after leaving it.)\n";

while (true) {
    $message = $consumer->consume(1000);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $io = new AvroStringIO($message->payload);

            $reader = new AvroIODatumReader($schema);
            $dataReader = new AvroDataIOReader($io, $reader);

            $message->payload = $dataReader->data();

            var_dump($message);
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            echo "No more messages; will wait for more\n";
            break;
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            echo "Timed out\n";
            break;
        default:
            throw new \Exception($message->errstr(), $message->err);
            break;
    }
}

/*
$read_io = new AvroStringIO($binary_string);
$data_reader = new AvroDataIOReader($read_io, new AvroIODatumReader());
echo "from binary string:\n";
foreach ($data_reader->data() as $datum)
  echo var_export($datum, true) . "\n";
 */
