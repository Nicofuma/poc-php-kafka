<?php

require __DIR__.'/../vendor/autoload.php';

$schema = '';

$schema = <<<_JSON
{
    "namespace": "example.avro",
    "name": "member",
    "type": "record",
    "fields":
    [
        {"name": "time", "type": "long"},
        {"name": "site", "type": "string"},
        {"name": "ip", "type": "string"}
    ]
}
_JSON
;

$schema = AvroSchema::parse($schema);

$kafka = new \RdKafka\Producer();
$kafka->setLogLevel(LOG_DEBUG);
$kafka->addBrokers('kafka');

$topic = 'page_visits';
$nb = isset($argv[1]) ? $argv[1] : 1;

echo "Producing $nb messages to kafka topic '$topic'\n";
$topic = new AvroProducer($kafka->newTopic($topic),'http://schemaregistry:8081', null, $schema, ['register_missing_schemas' => true]);

$start = microtime(true);
for ($i = 0; $i < $nb ; $i++) {
    $format = mt_rand(0, 2);
    $format = $format === 2 ? null : $format;

    $topic->produce(RD_KAFKA_PARTITION_UA, 0, [
        'time' => time(),
        'site' => 'www.example.com',
        'ip' => '192.168.2.'.mt_rand(0, 255),
    ], null, null, null, $format);
}

$end = microtime(true);

echo 'Published: '.($end - $start)."\n";

/*

$io = new AvroStringIO();
$encoder = new AvroIOBinaryEncoder($io);
$writer = new AvroIODatumWriter($schema);
foreach ($data as $datum) {
    $writer->write($datum, $encoder);
}

OU

Etendre AvroDataIOWriter pour skip l'écriture des en-têtes
 */
