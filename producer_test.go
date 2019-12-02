package kafkaavro_test

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaavro "github.com/mycujoo/go-kafka-avro"
)

func TestNewProducer(t *testing.T) {
	kp, err := kafka.NewProducer(&kafka.ConfigMap{
		"socket.timeout.ms":    1100,
		"default.topic.config": kafka.ConfigMap{"message.timeout.ms": 10}})
	if err != nil {
		t.Errorf("Error creating kafka producer: %+v", err.Error())
	}

	srClient := &mockSchemaRegistryClient{}

	p, err := kafkaavro.NewProducer(kafkaavro.ProducerConfig{
		TopicName:            "topic",
		KeySchema:            `"string"`,
		ValueSchema:          `"string"`,
		Producer:             kp,
		SchemaRegistryClient: srClient,
	})
	if err != nil {
		t.Fatalf("Error creating producer: %+v", err.Error())
	}

	err = p.Produce("key", "value", nil)
	if err == nil || err.Error() != "Local: Message timed out" {
		t.Errorf("Expected timeout error")
	}

	err = p.Produce("key", struct{ ID string }{ID: "id"}, nil)
	if err == nil {
		t.Fatalf("Expected error for message")
	}
	if err.Error() != "cannot encode binary bytes: expected: string; received: struct { ID string }" {
		t.Fatalf("Unexpected error: %+v", err)
	}

	p.Close()
}
