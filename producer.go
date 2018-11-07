package kafkaavro

import (
	"encoding/binary"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
)

type Producer struct {
	producer       *kafka.Producer
	topicPartition kafka.TopicPartition

	schemaRegistryClient       SchemaRegistryClient
	schemaRegistrySubjectKey   string
	schemaRegistrySubjectValue string

	keySchemaID   int
	valueSchemaID int

	avroKeyCodec   *goavro.Codec
	avroValueCodec *goavro.Codec
}

// NewProducer is a producer that publishes messages to kafka topic using avro serialization format
func NewProducer(topic string, keySchema string, valueSchema string, producer *kafka.Producer, schemaRegistryClient SchemaRegistryClient) (*Producer, error) {

	if producer == nil {
		return nil, errors.New("missing producer")
	}
	if schemaRegistryClient == nil {
		return nil, errors.New("missing schema registry client")
	}

	keyCodec, err := goavro.NewCodec(keySchema)
	if err != nil {
		return nil, errors.Wrap(err, "cannot initialize key codec")
	}

	valueCodec, err := goavro.NewCodec(valueSchema)
	if err != nil {
		return nil, errors.Wrap(err, "cannot initialize value codec")
	}

	keySchemaID, err := schemaRegistryClient.RegisterNewSchema(topic+"-key", keyCodec)
	if err != nil {
		return nil, err
	}
	valueSchemaID, err := schemaRegistryClient.RegisterNewSchema(topic+"-value", valueCodec)
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer:       producer,
		topicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},

		schemaRegistryClient:       schemaRegistryClient,
		schemaRegistrySubjectKey:   topic + "-key",
		schemaRegistrySubjectValue: topic + "-value",

		keySchemaID:   keySchemaID,
		valueSchemaID: valueSchemaID,

		avroKeyCodec:   keyCodec,
		avroValueCodec: valueCodec,
	}, nil
}

// Produce will try to publish message to a topic. If deliveryChan is provided then function will return immediately,
// otherwise it will wait for delivery
func (ap *Producer) Produce(key interface{}, value interface{}, deliveryChan chan kafka.Event) error {

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
