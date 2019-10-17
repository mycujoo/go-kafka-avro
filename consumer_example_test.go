package kafkaavro_test

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	kafkaavro "github.com/mycujoo/go-kafka-avro"
)

func ExampleNewConsumer() {
	kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:29092",
		"security.protocol":        "ssl",
		"socket.keepalive.enable":  true,
		"enable.auto.commit":       false,
		"ssl.key.location":         "/path/to/service.key",
		"ssl.certificate.location": "/path/to/service.cert",
		"ssl.ca.location":          "/path/to/ca.pem",
		"group.id":                 "some-group-id",
		"session.timeout.ms":       6000,
		"default.topic.config":     kafka.ConfigMap{"auto.offset.reset": "earliest"},
	})
	if err != nil {
		log.Fatal(err)
	}
	cachedSchemaRegistry, err := kafkaavro.NewCachedSchemaRegistryClient("http://localhost:8081")
	if err != nil {
		log.Fatal(err)
	}

	kafkaavro.NewConsumer([]string{"topic1"}, kafkaConsumer, cachedSchemaRegistry)
}
