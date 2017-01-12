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

$topic = new AvroProducer($kafka->newTopic('page_visits'),'http://schemaregistry:8081', null, $schema);

$nb = isset($argv[1]) ? $argv[1] : 1;

$start = microtime(true);
for ($i = 0; $i < $nb ; $i++) {
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, [
        'time' => time(),
        'site' => 'www.example.com',
        'ip' => '192.168.2.'.mt_rand(0, 255),
    ]);
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
