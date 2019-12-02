package kafkaavro

import (
	"encoding/binary"

	"github.com/cenkalti/backoff/v3"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
)

type kafkaProducer interface {
	Close()
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
}

type Producer struct {
	producer       kafkaProducer
	topicPartition kafka.TopicPartition

	schemaRegistryClient       SchemaRegistryClient
	schemaRegistrySubjectKey   string
	schemaRegistrySubjectValue string

	keySchemaID   int
	valueSchemaID int

	avroKeyCodec   *goavro.Codec
	avroValueCodec *goavro.Codec

	backOffConfig backoff.BackOff
}

type ProducerConfig struct {
	// Name of the topic where messages will be produced
	TopicName string

	// Avro schema for message key
	KeySchema string

	// Avro schema for message value
	ValueSchema string

	// Low level kafka producer used to produce messages
	Producer kafkaProducer

	// Schema registry client used for messages validation and schema management
	SchemaRegistryClient SchemaRegistryClient

	// BackOffConfig is used for setting backoff strategy for retry logic
	BackOffConfig backoff.BackOff
}

// NewProducer is a producer that publishes messages to kafka topic using avro serialization format
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if cfg.Producer == nil {
		return nil, errors.New("missing producer")
	}

	if cfg.SchemaRegistryClient == nil {
		return nil, errors.New("missing schema registry client")
	}

	keyCodec, err := goavro.NewCodec(cfg.KeySchema)
	if err != nil {
		return nil, errors.Wrap(err, "cannot initialize key codec")
	}

	valueCodec, err := goavro.NewCodec(cfg.ValueSchema)
	if err != nil {
		return nil, errors.Wrap(err, "cannot initialize value codec")
	}

	schemaRegistrySubjectKey := cfg.TopicName + "-key"
	keySchemaID, err := cfg.SchemaRegistryClient.RegisterNewSchema(schemaRegistrySubjectKey, keyCodec)
	if err != nil {
		return nil, err
	}

	schemaRegistrySubjectValue := cfg.TopicName + "-value"
	valueSchemaID, err := cfg.SchemaRegistryClient.RegisterNewSchema(schemaRegistrySubjectValue, valueCodec)
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer: cfg.Producer,
		topicPartition: kafka.TopicPartition{
			Topic:     &cfg.TopicName,
			Partition: kafka.PartitionAny,
		},

		schemaRegistryClient:       cfg.SchemaRegistryClient,
		schemaRegistrySubjectKey:   schemaRegistrySubjectKey,
		schemaRegistrySubjectValue: schemaRegistrySubjectValue,

		keySchemaID:   keySchemaID,
		valueSchemaID: valueSchemaID,

		avroKeyCodec:   keyCodec,
		avroValueCodec: valueCodec,

		backOffConfig: cfg.BackOffConfig,
	}, nil
}

// Produce will try to publish message to a topic. If deliveryChan is provided then function will return immediately,
// otherwise it will wait for delivery
func (ap *Producer) produce(key interface{}, value interface{}, deliveryChan chan kafka.Event) error {
	binaryKey, err := getAvroBinary(ap.keySchemaID, ap.avroKeyCodec, key)
	if err != nil {
		return err
	}

	binaryValue, err := getAvroBinary(ap.valueSchemaID, ap.avroValueCodec, value)
	if err != nil {
		return err
	}

	handleError := false
	if deliveryChan == nil {
		handleError = true
		deliveryChan = make(chan kafka.Event)
	}

	msg := &kafka.Message{
		TopicPartition: ap.topicPartition,
		Key:            binaryKey,
		Value:          binaryValue,
	}
	ap.producer.Produce(msg, deliveryChan)

	if handleError {
		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}
	}

	return nil
}

func (ap *Producer) Produce(key interface{}, value interface{}, deliveryChan chan kafka.Event) error {
	if ap.backOffConfig != nil {
		return backoff.Retry(func() error {
			return ap.produce(key, value, deliveryChan)
		}, ap.backOffConfig)
	}

	return ap.produce(key, value, deliveryChan)
}

func getAvroBinary(schemaID int, codec *goavro.Codec, native interface{}) ([]byte, error) {
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schemaID))

	// Convert native Go form to binary Avro data
	binaryValue, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}

	binaryMsg := make([]byte, 0, len(binaryValue)+5)
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, binarySchemaId...)
	// avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binaryValue...)

	return binaryMsg, nil
}

func (ac *Producer) Close() {
	ac.producer.Close()
}
