<?php

use Guzzle\Http\Client;

/**
 * Client that talk to a schema registry over http
 *
 * See http://confluent.io/docs/current/schema-registry/docs/intro.html
 * See https://github.com/confluentinc/confluent-kafka-python
 */
class CachedSchemaRegistryClient
{
    private $maxSchemasPerSubject;

    private $client;

    /** @var SplObjectStorage[] */
    private $subjectToSchemaIds = [];
    private $idToSchema = [];
    private $subjectToSchemaVersions = [];

    /**
     * @param string $url
     * @param int $maxSchemasPerSubject
     */
    public function __construct($url, $maxSchemasPerSubject = 1000)
    {
        $this->maxSchemasPerSubject = $maxSchemasPerSubject;

        $this->client = new Client($url);
    }

    /**
     * POST /subjects/(string: subject)/versions
     * Register a schema with the registry under the given subject
     * and receive a schema id.
     *
     * $schema must be a parsed schema from the php avro library
     *
     * Multiple instances of the same schema will result in cache misses.
     *
     * @param string $subject Subject name
     * @param AvroSchema $schema Avro schema to be registered
     *
     * @return int
     */
    public function register($subject, AvroSchema $schema)
    {
        if (isset($this->subjectToSchemaIds[$subject])) {
            $schemasToId = $this->subjectToSchemaIds[$subject];

            if (isset($schemasToId[$schema])) {
                return $schemasToId[$schema];
            }
        }

        $url = sprintf('/subjects/%s/versions', $subject);
        list($status, $response) = $this->sendRequest($url, 'POST', json_encode(['schema' => (string) $schema]));

        if ($status === 409) {
            throw new RuntimeException('Incompatible Avro schema');
        } else if ($status === 422) {
            throw new RuntimeException('Invalid Avro schema');
        } else if (!($status >= 200 || $status < 300)) {
            throw new \RuntimeException('Unable to register schema. Error code: '.$status);
        }

        $schemaId = $response['id'];
        $this->cacheSchema($schema, $schemaId, $subject);

        return $schemaId;
    }

    /**
     * GET /schemas/ids/{int: id}
     * Retrieve a parsed avro schema by id or None if not found
     *
     * @param int $schemaId value
     *
     * @return AvroSchema Avro schema
     */
    public function getById($schemaId)
    {
        if (isset($this->idToSchema[$schemaId])) {
            return $this->idToSchema[$schemaId];
        }

        $url = sprintf('/schemas/ids/%d', $schemaId);
        list($status, $response) = $this->sendRequest($url, 'GET');

        if ($status === 404) {
            throw new RuntimeException('Schema not found');
        } else if (!($status >= 200 || $status < 300)) {
            throw new \RuntimeException('Unable to get schema for the specific ID: '.$status);
        }

        $schema = AvroSchema::parse($response['schema']);

        $this->cacheSchema($schema, $schemaId);

        return $schema;
    }

    private function sendRequest($url, $method = 'GET', $body = null, $headers = null)
    {
        $headers = (array) $headers;
        $headers['Accept'] = 'application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json';

        if ($body) {
            $headers['Content-Type'] = 'application/vnd.schemaregistry.v1+json';
        }

        switch ($method) {
            case 'GET':
                $request = $this->client->get($url, $headers);
                break;
            case 'POST':
                $request = $this->client->post($url, $headers, $body);
                break;
            case 'PUT':
                $request = $this->client->put($url, $headers, $body);
                break;
            case 'DELETE':
                $request = $this->client->delete($url, $headers);
                break;
            default:
                throw new \RuntimeException('Invalid HTTP method');
        }

        $response = $this->client->send($request);

        return [$response->getStatusCode(), json_decode($response->getBody(true), true)];
    }

    /**
     * @param AvroSchema $schema
     * @param int $schemaId
     * @param string|null $subject
     * @param string|null $version
     */
    private function cacheSchema(AvroSchema $schema, $schemaId, $subject = null, $version = null)
    {
        if (isset($this->idToSchema[$schemaId])) {
            $schema = $this->idToSchema[$schemaId];
        } else {
            $this->idToSchema[$schemaId] = $schema;
        }

        if ($subject) {
            $this->addToCache($this->subjectToSchemaIds, $subject, $schema, $schemaId);

            if ($version) {
                $this->addToCache($this->subjectToSchemaVersions, $subject, $schema, $version);
            }
        }
    }

    /**
     * @param \SplObjectStorage[] $cache
     * @param string $subject
     * @param AvroSchema $schema
     * @param string $value
     */
    private function addToCache(&$cache, $subject, AvroSchema $schema, $value)
    {
        if (!isset($cache[$subject])) {
            $cache[$subject] = new \SplObjectStorage();
        }

        $subCache = $cache[$subject];
        $subCache[$subCache] = $value;
    }
}
