<?php

class MessageSerializer
{
    const MAGIC_BYTE = 0;

    private $idToDecoderFunc = [];

    /** @var AvroIODatumWriter[] */
    private $idToWriters = [];

    /** @var CachedSchemaRegistryClient */
    private $registry;

    public function __construct(CachedSchemaRegistryClient $registry)
    {
        $this->registry = $registry;
    }

    /**
     * Encode a record with a given schema id.
     *
     * @param int $schemaId
     * @param array $record A data to serialize
     * @param bool $isKey If the record is a key
     *
     * @return AvroIODatumWriter encoder object
     */
    public function encodeRecordWithSchemaId($schemaId, array $record, $isKey = false)
    {
        if (! isset($this->idToWriters[$schemaId])) {
            $schema = $this->registry->getById($schemaId);

            $this->idToWriters[$schemaId] = new AvroIODatumWriter($schema);
        }

        $writer = $this->idToWriters[$schemaId];

        $io = new AvroStringIO();

        // write the header

        // magic byte
        $io->write(pack('c', static::MAGIC_BYTE));

        // write the schema ID in network byte order (big end)
        $io->write(pack('N', $schemaId));

        // write the record to the rest of it
        // Create an encoder that we'll write to
        $encoder = new AvroIOBinaryEncoder($io);

        // write the magic byte
        // write the object in 'obj' as Avro to the fake file...
        $writer->write($record, $encoder);

        return $io->string();
    }

    /**
     * Given a parsed avro schema, encode a record for the given topic.
     * The schema is registered with the subject of 'topic-value'
     *
     * @param string $topic Topic name
     * @param AvroSchema $schema Avro Schema
     * @param array $record An object to serialize
     * @param bool $isKey If the record is a key
     *
     * @return string Encoded record with schema ID as bytes
     */
    public function encodeRecordWithSchema($topic, AvroSchema $schema, array $record, $isKey = false)
    {
        $suffix = $isKey ? '-key' : '-value';
        $subject = $topic.$suffix;

        $schemaId = $this->registry->register($subject, $schema);

        $this->idToWriters[$schemaId] = new AvroIODatumWriter($schema);

        return $this->encodeRecordWithSchemaId($schemaId, $record, $isKey);
    }
}
