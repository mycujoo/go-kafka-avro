package kafkaavro

import (
	"sync"

	schemaregistry "github.com/landoop/schema-registry"
	"github.com/linkedin/goavro"
)

// Portions of the code are taken from https://github.com/dangkaka/go-kafka-avro

type SchemaRegistryClient interface {
	GetSchemaByID(id int) (*goavro.Codec, error)
	RegisterNewSchema(subject string, codec *goavro.Codec) (int, error)
}

// CachedSchemaRegistryClient is a schema registry client that will cache some data to improve performance
type CachedSchemaRegistryClient struct {
	SchemaRegistryClient *schemaregistry.Client
	schemaCache          map[int]*goavro.Codec
	schemaCacheLock      sync.RWMutex
	schemaIdCache        map[string]int
	schemaIdCacheLock    sync.RWMutex
}

func NewCachedSchemaRegistryClient(baseURL string, options ...schemaregistry.Option) (*CachedSchemaRegistryClient, error) {
	srClient, err := schemaregistry.NewClient(baseURL, options...)
	if err != nil {
		return nil, err
	}
	return &CachedSchemaRegistryClient{
		SchemaRegistryClient: srClient,
		schemaCache:          make(map[int]*goavro.Codec),
		schemaIdCache:        make(map[string]int),
	}, nil
}

// GetSchemaByID will return and cache the codec with the given id
func (cached *CachedSchemaRegistryClient) GetSchemaByID(id int) (*goavro.Codec, error) {
	cached.schemaCacheLock.RLock()
	cachedResult := cached.schemaCache[id]
	cached.schemaCacheLock.RUnlock()
	if nil != cachedResult {
		return cachedResult, nil
	}
	schema, err := cached.SchemaRegistryClient.GetSchemaByID(id)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	cached.schemaCacheLock.Lock()
	cached.schemaCache[id] = codec
	cached.schemaCacheLock.Unlock()
	return codec, nil
}

// Subjects returns a list of subjects
func (cached *CachedSchemaRegistryClient) Subjects() ([]string, error) {
	return cached.SchemaRegistryClient.Subjects()
}

// Versions returns a list of all versions of a subject
func (cached *CachedSchemaRegistryClient) Versions(subject string) ([]int, error) {
	return cached.SchemaRegistryClient.Versions(subject)
}

// GetSchemaBySubject returns the codec for a specific version of a subject
func (cached *CachedSchemaRegistryClient) GetSchemaBySubject(subject string, version int) (*goavro.Codec, error) {
	schema, err := cached.SchemaRegistryClient.GetSchemaBySubject(subject, version)
	if err != nil {
		return nil, err
	}
	return goavro.NewCodec(schema.Schema)
}

// GetLatestSchema returns the highest version schema for a subject
func (cached *CachedSchemaRegistryClient) GetLatestSchema(subject string) (*goavro.Codec, error) {
	schema, err := cached.SchemaRegistryClient.GetLatestSchema(subject)
	if err != nil {
		return nil, err
	}
	return goavro.NewCodec(schema.Schema)
}

// RegisterNewSchema will return and cache the id with the given codec
func (cached *CachedSchemaRegistryClient) RegisterNewSchema(subject string, codec *goavro.Codec) (int, error) {
	schemaJson := codec.Schema()
	cached.schemaIdCacheLock.RLock()
	cachedResult, found := cached.schemaIdCache[schemaJson]
	cached.schemaIdCacheLock.RUnlock()
	if found {
		return cachedResult, nil
	}
	id, err := cached.SchemaRegistryClient.RegisterNewSchema(subject, schemaJson)
	if err != nil {
		return 0, err
	}
	cached.schemaIdCacheLock.Lock()
	cached.schemaIdCache[schemaJson] = id
	cached.schemaIdCacheLock.Unlock()
	return id, nil
}

// IsSchemaRegistered checks if a specific codec is already registered to a subject
func (cached *CachedSchemaRegistryClient) IsSchemaRegistered(subject string, codec *goavro.Codec) (bool, schemaregistry.Schema, error) {
	return cached.SchemaRegistryClient.IsRegistered(subject, codec.Schema())
}

// DeleteSubject deletes the subject, should only be used in development
func (cached *CachedSchemaRegistryClient) DeleteSubject(subject string) (versions []int, err error) {
	return cached.SchemaRegistryClient.DeleteSubject(subject)
}
