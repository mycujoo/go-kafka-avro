package kafkaavro

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

type kafkaConsumer interface {
	Poll(timeoutMs int) (event kafka.Event)
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	Close() (err error)
	CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
}

type Consumer struct {
	consumer             kafkaConsumer
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

	// JSON representation of the message
	Textual []byte
}

// NewConsumer is a basic consumer to interact with schema registry, avro and kafka
func NewConsumer(topics []string, consumer kafkaConsumer, schemaRegistryClient SchemaRegistryClient) (*Consumer, error) {
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
					parsed, textual, err := ac.decodeAvroBinary(e.Value)

					if err != nil {
						output <- ConsumerMessage{
							Message: e,
							Error:   err,
						}

						continue
					}

					msg := ConsumerMessage{
						Message: e,
						Parsed:  parsed,
						Textual: textual,
					}

					if _, err = json.Marshal(msg.Parsed); err != nil {
						msg.Error = err
						output <- msg

						continue
					}

					if e.TopicPartition.Topic == nil {
						msg.Error = errors.New("null topic")
						output <- msg

						continue
					}

					if msg.Parsed == nil {
						msg.Error = errors.New("missing parsed data")
						output <- msg

						continue
					}

					if _, ok := msg.Parsed.(map[string]interface{}); !ok {
						msg.Error = errors.New("parsed data is wrong type")
						output <- msg

						continue
					}

					output <- ConsumerMessage{
						Message: e,
						Parsed:  parsed,
						Textual: textual,
					}

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

func (ac *Consumer) decodeAvroBinary(data []byte) (interface{}, []byte, error) {
	if data[0] != 0 {
		return nil, nil, errors.New("invalid magic byte")
	}
	schemaId := binary.BigEndian.Uint32(data[1:5])
	codec, err := ac.schemaRegistryClient.GetSchemaByID(int(schemaId))
	if err != nil {
		return nil, nil, err
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(data[5:])
	if err != nil {
		return nil, nil, err
	}

	textual, err := codec.TextualFromNative(nil, native)

	if err != nil {
		return nil, nil, err
	}

	return native, textual, err
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
