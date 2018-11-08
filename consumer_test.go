package kafkaavro_test

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mycujoo/go-kafka-avro"
	"testing"
)

func TestConsumer(t *testing.T) {

	kc, err := kafka.NewConsumer(&kafka.ConfigMap{
		"group.id":                 "gotest",
		"socket.timeout.ms":        10,
		"session.timeout.ms":       10,
		"enable.auto.offset.store": false, // permit StoreOffsets()
	})
	if err != nil {
		t.Fatalf("%s", err)
	}

	srClient := &mockSchemaRegistryClient{}

	c, err := kafkaavro.NewConsumer([]string{"topic1"}, kc, srClient)
	if err != nil {
		t.Errorf("Subscribe failed: %s", err)
	}
	ch := make(chan struct{})
	c.Messages(ch)
	close(ch)

	t.Logf("Consumer %v", c)
}
