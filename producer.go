package kafkaavro

import (
	"encoding/binary"
	"net/url"

	"github.com/caarlos0/env/v6"
	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro/v2"
	"github.com/pkg/errors"
)

type KafkaProducer interface {
	Close()
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
}

type Producer struct {
	KafkaProducer
	topicPartition kafka.TopicPartition

	avroAPI  avro.API
	kafkaCfg *kafka.ConfigMap
	srURL    *url.URL
	srClient SchemaRegistryClient

	keySchemaID   int
	valueSchemaID int

	avroKeySchema   avro.Schema
	avroValueSchema avro.Schema

	backOffConfig backoff.BackOff
}

// NewProducer is a producer that publishes messages to kafka topic using avro serialization format
func NewProducer(
	topicName string,
	keySchemaJSON, valueSchemaJSON string,
	opts ...ProducerOption,
) (*Producer, error) {
	p := &Producer{
		avroAPI: avro.DefaultConfig,
	}
	// Loop through each option
	for _, opt := range opts {
		// apply option
		opt.applyP(p)
	}

	var err error

	// if producer not provided - make one
	if p.KafkaProducer == nil {
		// if kafka config not provided - build default one
		if p.kafkaCfg == nil {
			var envCfg struct {
				Broker          string `env:"KAFKA_BROKER" envDefault:"localhost:9092"`
				CAFile          string `env:"KAFKA_CA_FILE"`
				KeyFile         string `env:"KAFKA_KEY_FILE"`
				CertificateFile string `env:"KAFKA_CERTIFICATE_FILE"`
			}
			if err := env.Parse(&envCfg); err != nil {
				return nil, err
			}

			// default configuration
			p.kafkaCfg = &kafka.ConfigMap{
				"bootstrap.servers":       envCfg.Broker,
				"socket.keepalive.enable": true,
				"log.connection.close":    false,
			}

			if envCfg.CAFile != "" {
				// configure SSL
				p.kafkaCfg.SetKey("security.protocol", "ssl")
				p.kafkaCfg.SetKey("ssl.ca.location", envCfg.CAFile)
				p.kafkaCfg.SetKey("ssl.key.location", envCfg.KeyFile)
				p.kafkaCfg.SetKey("ssl.certificate.location", envCfg.CertificateFile)
			}
		}

		if p.KafkaProducer, err = kafka.NewProducer(p.kafkaCfg); err != nil {
			return nil, errors.WithMessage(err, "cannot initialize kafka producer")
		}
	}

	if p.srClient == nil {
		if p.srURL == nil {
			var envCfg struct {
				SchemaRegistry *url.URL `env:"KAFKA_SCHEMA_REGISTRY" envDefault:"http://localhost:8081"`
			}
			if err := env.Parse(&envCfg); err != nil {
				return nil, err
			}
			p.srURL = envCfg.SchemaRegistry
		}

		if p.srClient, err = NewCachedSchemaRegistryClient(p.srURL.String()); err != nil {
			return nil, errors.WithMessage(err, "cannot initialize schema registry client")
		}
	}

	p.avroKeySchema, err = avro.Parse(keySchemaJSON)
	if err != nil {
		return nil, errors.Wrap(err, "cannot initialize key codec")
	}

	p.avroValueSchema, err = avro.Parse(valueSchemaJSON)
	if err != nil {
		return nil, errors.Wrap(err, "cannot initialize value codec")
	}

	schemaRegistrySubjectKey := topicName + "-key"
	p.keySchemaID, err = p.srClient.RegisterNewSchema(schemaRegistrySubjectKey, p.avroKeySchema)
	if err != nil {
		return nil, err
	}

	schemaRegistrySubjectValue := topicName + "-value"
	p.valueSchemaID, err = p.srClient.RegisterNewSchema(schemaRegistrySubjectValue, p.avroValueSchema)
	if err != nil {
		return nil, err
	}

	p.topicPartition = kafka.TopicPartition{
		Topic:     &topicName,
		Partition: kafka.PartitionAny,
	}

	return p, nil
}

// Produce will try to publish message to a topic. If deliveryChan is provided then function will return immediately,
// otherwise it will wait for delivery
func (ap *Producer) produce(key interface{}, value interface{}, deliveryChan chan kafka.Event) error {
	binaryKey, err := ap.getAvroBinary(ap.keySchemaID, ap.avroKeySchema, key)
	if err != nil {
		return err
	}

	binaryValue, err := ap.getAvroBinary(ap.valueSchemaID, ap.avroValueSchema, value)
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
	if err = ap.KafkaProducer.Produce(msg, deliveryChan); err != nil {
		return err
	}

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

func (ap *Producer) getAvroBinary(schemaID int, schema avro.Schema, value interface{}) ([]byte, error) {
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schemaID))

	// Convert to binary Avro data
	binaryValue, err := ap.avroAPI.Marshal(schema, value)
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
