package kafkaavro

import (
	"encoding/json"
	"sync"

	"github.com/hamba/avro/v2"
	schemaregistry "github.com/landoop/schema-registry"
)

// Portions of the code are taken from https://github.com/dangkaka/go-kafka-avro

type SchemaRegistryClient interface {
	GetSchemaByID(id int) (avro.Schema, error)
	RegisterNewSchema(subject string, schema avro.Schema) (int, error)
}

// CachedSchemaRegistryClient is a schema registry client that will cache some data to improve performance
type CachedSchemaRegistryClient struct {
	SchemaRegistryClient   *schemaregistry.Client
	schemaCache            map[int]avro.Schema
	schemaCacheLock        sync.RWMutex
	registeredSubjects     map[string]int
	registeredSubjectsLock sync.RWMutex
}

func NewCachedSchemaRegistryClient(baseURL string, options ...schemaregistry.Option) (*CachedSchemaRegistryClient, error) {
	srClient, err := schemaregistry.NewClient(baseURL, options...)
	if err != nil {
		return nil, err
	}
	return &CachedSchemaRegistryClient{
		SchemaRegistryClient: srClient,
		schemaCache:          make(map[int]avro.Schema),
		registeredSubjects:   make(map[string]int),
	}, nil
}

// GetSchemaByID will return and cache the schema with the given id
func (cached *CachedSchemaRegistryClient) GetSchemaByID(id int) (avro.Schema, error) {
	cached.schemaCacheLock.RLock()
	cachedResult := cached.schemaCache[id]
	cached.schemaCacheLock.RUnlock()
	if nil != cachedResult {
		return cachedResult, nil
	}
	schemaJSON, err := cached.SchemaRegistryClient.GetSchemaByID(id)
	if err != nil {
		return nil, err
	}
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, err
	}
	cached.schemaCacheLock.Lock()
	cached.schemaCache[id] = schema
	cached.schemaCacheLock.Unlock()
	return schema, nil
}

// Subjects returns a list of subjects
func (cached *CachedSchemaRegistryClient) Subjects() ([]string, error) {
	return cached.SchemaRegistryClient.Subjects()
}

// Versions returns a list of all versions of a subject
func (cached *CachedSchemaRegistryClient) Versions(subject string) ([]int, error) {
	return cached.SchemaRegistryClient.Versions(subject)
}

// GetSchemaBySubject returns the schema for a specific version of a subject
func (cached *CachedSchemaRegistryClient) GetSchemaBySubject(subject string, version int) (avro.Schema, error) {
	schema, err := cached.SchemaRegistryClient.GetSchemaBySubject(subject, version)
	if err != nil {
		return nil, err
	}
	return avro.Parse(schema.Schema)
}

// GetLatestSchema returns the highest version schema for a subject
func (cached *CachedSchemaRegistryClient) GetLatestSchema(subject string) (avro.Schema, error) {
	schema, err := cached.SchemaRegistryClient.GetLatestSchema(subject)
	if err != nil {
		return nil, err
	}
	return avro.Parse(schema.Schema)
}

// RegisterNewSchema will return and cache the id with the given schema
func (cached *CachedSchemaRegistryClient) RegisterNewSchema(subject string, schema avro.Schema) (int, error) {
	cached.registeredSubjectsLock.RLock()
	cachedResult, found := cached.registeredSubjects[subject]
	cached.registeredSubjectsLock.RUnlock()
	if found {
		return cachedResult, nil
	}

	jsonBytes, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}

	id, err := cached.SchemaRegistryClient.RegisterNewSchema(subject, string(jsonBytes))
	if err != nil {
		return 0, err
	}

	cached.registeredSubjectsLock.Lock()
	cached.registeredSubjects[subject] = id
	cached.registeredSubjectsLock.Unlock()
	return id, nil
}

// IsSchemaRegistered checks if a specific schema is already registered to a subject
func (cached *CachedSchemaRegistryClient) IsSchemaRegistered(subject string, schema avro.Schema) (bool, schemaregistry.Schema, error) {
	return cached.SchemaRegistryClient.IsRegistered(subject, schema.String())
}

// DeleteSubject deletes the subject, should only be used in development
func (cached *CachedSchemaRegistryClient) DeleteSubject(subject string) (versions []int, err error) {
	return cached.SchemaRegistryClient.DeleteSubject(subject)
}
