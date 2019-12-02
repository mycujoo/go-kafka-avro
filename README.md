# go-kafka-avro

A wrapper for Confluent's libraries for [Apache Kafka](http://kafka.apache.org/) and Schema Registry.

## Installation

First install dependencies:

    go get github.com/confluentinc/confluent-kafka-go github.com/landoop/schema-registry

To install use `go get`:

    go get github.com/mycujoo/go-kafka-avro

## Usage

First, you need to create cached schema registry client:

    srClient, err := kafkaavro.NewCachedSchemaRegistryClient(baseurl)

For more options look at [Landoop Schema Registry Client README](https://github.com/Landoop/schema-registry#client).

### Producer

Create kafka producer:

    kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})

Then construct you can construct one or more kafkaavro producers:

    producer, err := kafkaavro.NewProducer(kafkaavro.ProducerConfig{
    	TopicName:            "topic",
    	KeySchema:            `"string"`,
    	ValueSchema:          `{"type": "record", "name": "test", "fields" : [{"name": "val", "type": "int", "default": 0}]}`,
    	Producer:             kafkaProducer,
    	SchemaRegistryClient: srClient,
    })

Publish message using `Produce` method:

    err = producer.Produce("key", "value", nil)

If you provide deliverChan then call will not be blocking until delivery.

## Supported go versions

We support only latest version, which is 1.11 at the moment.

## Related

Some code for cached schema registry client was based on https://github.com/dangkaka/go-kafka-avro implementation.
