package kafkaavro

import (
	"net/url"

	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro/v2"
)

type ConsumerOption interface {
	applyC(*Consumer)
}

type ProducerOption interface {
	applyP(*Producer)
}

type SharedOption interface {
	ConsumerOption
	ProducerOption
}

type options struct {
	avroAPI  avro.API
	kafkaCfg *kafka.ConfigMap
	srURL    *url.URL
	srClient SchemaRegistryClient
}

type funcSharedOption struct {
	fC func(*Consumer)
	fP func(*Producer)
}

func (f funcSharedOption) applyC(c *Consumer) {
	f.fC(c)
}

func (f funcSharedOption) applyP(p *Producer) {
	f.fP(p)
}

func WithAvroAPI(api avro.API) SharedOption {
	return funcSharedOption{
		func(o *Consumer) {
			o.avroAPI = api
		},
		func(o *Producer) {
			o.avroAPI = api
		},
	}
}

func WithKafkaConfig(cfg *kafka.ConfigMap) SharedOption {
	return funcSharedOption{
		func(o *Consumer) {
			o.kafkaCfg = cfg
		},
		func(o *Producer) {
			o.kafkaCfg = cfg
		},
	}
}

func WithSchemaRegistryURL(url *url.URL) SharedOption {
	return funcSharedOption{
		func(o *Consumer) {
			o.srURL = url
		},
		func(o *Producer) {
			o.srURL = url
		},
	}
}

func WithSchemaRegistryClient(srClient SchemaRegistryClient) SharedOption {
	return funcSharedOption{
		func(o *Consumer) {
			o.srClient = srClient
		},
		func(o *Producer) {
			o.srClient = srClient
		},
	}
}

type funcConsumerOption struct {
	f func(*Consumer)
}

func (o funcConsumerOption) applyC(c *Consumer) {
	o.f(c)
}

func WithKafkaConsumer(consumer KafkaConsumer) ConsumerOption {
	return funcConsumerOption{func(o *Consumer) {
		o.KafkaConsumer = consumer
	}}
}

func WithoutTopicsCheck() ConsumerOption {
	return funcConsumerOption{func(o *Consumer) {
		o.ensureTopics = false
	}}
}

func WithEventHandler(handler EventHandler) ConsumerOption {
	return funcConsumerOption{func(o *Consumer) {
		o.eventHandler = handler
	}}
}

type funcProducerOption struct {
	f func(*Producer)
}

func (o funcProducerOption) applyP(p *Producer) {
	o.f(p)
}

func WithKafkaProducer(producer KafkaProducer) ProducerOption {
	return funcProducerOption{func(o *Producer) {
		o.KafkaProducer = producer
	}}
}

func WithBackoff(backOff backoff.BackOff) ProducerOption {
	return funcProducerOption{func(o *Producer) {
		o.backOffConfig = backOff
	}}
}
