package kafkaavro

import (
	"encoding/binary"
	"fmt"
	"log"
	"net/url"

	"github.com/caarlos0/env/v6"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
	"github.com/pkg/errors"
)

type KafkaConsumer interface {
	CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	Poll(timeoutMs int) kafka.Event
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
}

type Consumer struct {
	KafkaConsumer
	valueFactory ValueFactory
	eventHandler EventHandler
	ensureTopics bool
	avroAPI      avro.API
	kafkaCfg     *kafka.ConfigMap
	srURL        *url.URL
	srClient     SchemaRegistryClient
}

type ValueFactory func(topic string) interface{}
type EventHandler func(event kafka.Event)

type Message struct {
	*kafka.Message
	Value interface{}
}

// NewConsumer is a basic consumer to interact with schema registry, avro and kafka
func NewConsumer(topics []string, valueFactory ValueFactory, opts ...ConsumerOption) (*Consumer, error) {
	c := &Consumer{
		valueFactory: valueFactory,
		avroAPI:      avro.DefaultConfig,
		ensureTopics: true,
	}
	// Loop through each option
	for _, opt := range opts {
		// apply option
		opt.applyC(c)
	}

	var err error

	// if consumer not provided - make one
	if c.KafkaConsumer == nil {
		// if kafka config not provided - build default one
		if c.kafkaCfg == nil {
			var envCfg struct {
				Broker          string `env:"KAFKA_BROKER" envDefault:"localhost:9092"`
				CAFile          string `env:"KAFKA_CA_FILE"`
				KeyFile         string `env:"KAFKA_KEY_FILE"`
				CertificateFile string `env:"KAFKA_CERTIFICATE_FILE"`
				GroupID         string `env:"KAFKA_GROUP_ID"`
			}
			if err := env.Parse(&envCfg); err != nil {
				return nil, err
			}

			// default configuration
			c.kafkaCfg = &kafka.ConfigMap{
				"bootstrap.servers":       envCfg.Broker,
				"socket.keepalive.enable": true,
				"enable.auto.commit":      false,
				"enable.partition.eof":    true,
				"session.timeout.ms":      6000,
				"auto.offset.reset":       "earliest",
				"group.id":                envCfg.GroupID,
			}

			if envCfg.CAFile != "" {
				// configure SSL
				c.kafkaCfg.SetKey("security.protocol", "ssl")
				c.kafkaCfg.SetKey("ssl.ca.location", envCfg.CAFile)
				c.kafkaCfg.SetKey("ssl.key.location", envCfg.KeyFile)
				c.kafkaCfg.SetKey("ssl.certificate.location", envCfg.CertificateFile)
			}
		}

		if c.KafkaConsumer, err = kafka.NewConsumer(c.kafkaCfg); err != nil {
			return nil, errors.WithMessage(err, "cannot initialize kafka consumer")
		}
	}

	if c.srClient == nil {
		if c.srURL == nil {
			var envCfg struct {
				SchemaRegistry *url.URL `env:"KAFKA_SCHEMA_REGISTRY" envDefault:"http://localhost:8081"`
			}
			if err := env.Parse(&envCfg); err != nil {
				return nil, err
			}
			c.srURL = envCfg.SchemaRegistry
		}

		if c.srClient, err = NewCachedSchemaRegistryClient(c.srURL.String()); err != nil {
			return nil, errors.WithMessage(err, "cannot initialize schema registry client")
		}
	}

	if c.eventHandler == nil {
		c.eventHandler = func(event kafka.Event) {
			log.Println(event)
		}
	}

	if topics != nil {
		if err := c.KafkaConsumer.SubscribeTopics(topics, nil); err != nil {
			return nil, err
		}

		if c.ensureTopics {
			if err = c.EnsureTopics(topics); err != nil {
				return nil, err
			}
		}
	}

	return c, nil
}

func (ac *Consumer) fetchMessage(timeoutMs int) (*kafka.Message, error) {
	ev := ac.KafkaConsumer.Poll(timeoutMs)
	if ev == nil {
		return nil, nil
	}
	switch e := ev.(type) {
	case *kafka.Message:
		return e, nil
	default:
		ac.eventHandler(e)
		return nil, nil
	}
}

func (ac *Consumer) FetchMessage(timeoutMs int) (*Message, error) {
	msg, err := ac.fetchMessage(timeoutMs)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}

	value := ac.valueFactory(*msg.TopicPartition.Topic)
	if value == nil {
		return nil, ErrInvalidValue{Topic: *msg.TopicPartition.Topic}
	}

	err = ac.decodeAvroBinary(msg.Value, &value)
	return &Message{
		Message: msg,
		Value:   value,
	}, err
}

func (ac *Consumer) ReadMessage(timeoutMs int) (*Message, error) {
	msg, err := ac.FetchMessage(timeoutMs)
	if err != nil {
		return nil, err
	}
	if msg != nil { // FetchMessage may return a nil msg
		if _, err = ac.KafkaConsumer.CommitMessage(msg.Message); err != nil {
			err = ErrFailedCommit{Err: err}
		}
	}
	return msg, err
}

func (ac *Consumer) decodeAvroBinary(data []byte, v interface{}) error {
	if data[0] != 0 {
		return errors.New("invalid magic byte")
	}
	schemaId := binary.BigEndian.Uint32(data[1:5])
	schema, err := ac.srClient.GetSchemaByID(int(schemaId))
	if err != nil {
		return err
	}

	return ac.avroAPI.Unmarshal(schema, data[5:], v)
}

// EnsureTopics returns error if one of the consumed topics
// was not found on the server.
func (ac *Consumer) EnsureTopics(topics []string) error {
	notFound := make([]string, 0)

	meta, err := ac.KafkaConsumer.GetMetadata(nil, true, 6000)
	if err != nil {
		return err
	}

	for _, topic := range topics {
		if _, ok := meta.Topics[topic]; !ok {
			notFound = append(notFound, topic)
		}
	}

	if len(notFound) > 0 {
		return fmt.Errorf("topics not found: %v", notFound)
	}

	return nil
}
