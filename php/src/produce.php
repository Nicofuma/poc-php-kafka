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

$jose = ['id' => 1392, 'name' => 'Jose'];
$maria = ['id' => 1642, 'name' => 'Maria'];
$data = [$jose, $maria];

$kafka = new \RdKafka\Producer();
$kafka->setLogLevel(LOG_DEBUG);
$kafka->addBrokers('kafka');

$topic = new AvroProducer($kafka->newTopic('members'), 'http://schemaregistry:8081', null, $schema);

$nb = isset($argv[1]) ? $argv[1] : 1;

for ($i = 0; $i < $nb ; $i++) {
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $jose);
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $maria);
}

echo "Published\n";

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
