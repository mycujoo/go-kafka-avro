package kafkaavro

import (
	"encoding/binary"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

type Consumer struct {
	consumer             *kafka.Consumer
	schemaRegistryClient SchemaRegistryClient
	stopChan             chan struct{}
	pollTimeout          int
	topics               []string
}

type ConsumerMessage struct {
	*kafka.Message
	Error error
	// Message Value parsed into maps/structs
	Parsed interface{}
}

// NewConsumer is a basic consumer to interact with schema registry, avro and kafka
func NewConsumer(topics []string, consumer *kafka.Consumer, schemaRegistryClient SchemaRegistryClient) (*Consumer, error) {

	if topics != nil {
		if err := consumer.SubscribeTopics(topics, nil); err != nil {
			return nil, err
		}
	}

	return &Consumer{
		consumer:             consumer,
		schemaRegistryClient: schemaRegistryClient,
		pollTimeout:          100,
		topics:               topics,
	}, nil
}

func (ac *Consumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	ac.topics = topics
	return ac.consumer.SubscribeTopics(topics, rebalanceCb)
}

// Messages returns the ConsumerMessage channel (that contains decoded messages)
// and other events channel for events like kafka.PartitionEOF, kafka.Stats
func (ac *Consumer) Messages(stopChan chan struct{}) (chan ConsumerMessage, chan kafka.Event) {
	output := make(chan ConsumerMessage)
	other := make(chan kafka.Event)
	if ac.stopChan != nil {
		// stop channel already open
		close(ac.stopChan)
	}
	ac.stopChan = stopChan
	go func() {
		run := true
		for run {
			select {
			case <-stopChan:
				run = false

			default:
				ev := ac.consumer.Poll(ac.pollTimeout)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					msg := ConsumerMessage{
						Message: e,
					}
					msg.Parsed, msg.Error = ac.decodeAvroBinary(e.Value)
					output <- msg
				default:
					other <- e
				}
			}
		}
	}()
	return output, other
}

func (ac *Consumer) CommitMessage(msg ConsumerMessage) ([]kafka.TopicPartition, error) {
	return ac.consumer.CommitMessage(msg.Message)
}

func (ac *Consumer) Close() {
	ac.consumer.Close()
	close(ac.stopChan)
}

func (ac *Consumer) decodeAvroBinary(data []byte) (interface{}, error) {
	if data[0] != 0 {
		return nil, errors.New("invalid magic byte")
	}
	schemaId := binary.BigEndian.Uint32(data[1:5])
	codec, err := ac.schemaRegistryClient.GetSchemaByID(int(schemaId))
	if err != nil {
		return nil, err
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(data[5:])
	if err != nil {
		return nil, err
	}
	return native, err
}

// EnsureTopics returns error if one of the consumed topics
// was not found on the server.
func (ac *Consumer) EnsureTopics() error {
	notFound := make([]string, 0)

	meta, err := ac.consumer.GetMetadata(nil, true, 6000)
	if err != nil {
		return err
	}

	for _, topic := range ac.topics {
		if _, ok := meta.Topics[topic]; !ok {
			notFound = append(notFound, topic)
		}
	}

	if len(notFound) > 0 {
		return fmt.Errorf("topics not found: %v", notFound)
	}

	return nil
}
