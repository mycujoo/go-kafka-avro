package kafkaavro_test

import (
	"encoding/json"
	"fmt"
	"github.com/mycujoo/go-kafka-avro"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/linkedin/goavro"
)

type TestObject struct {
	MockServer *httptest.Server
	Codec      *goavro.Codec
	Subject    string
	ID         int
	Count      int
}

type schemaVersionResponse struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
	ID      int    `json:"id"`
}

type idResponse struct {
	ID int `json:"id"`
}

const (
	schemaByID       = "/schemas/ids/%d"
	subjects         = "/subjects"
	subjectVersions  = "/subjects/%s/versions"
	deleteSubject    = "/subjects/%s"
	subjectByVersion = "/subjects/%s/versions/%s"
)

func createSchemaRegistryTestObject(t *testing.T, subject string, id int) *TestObject {
	testObject := &TestObject{}
	testObject.Subject = subject
	testObject.ID = id
	testObject.Count = 0
	codec, err := goavro.NewCodec(`{"type": "record", "name": "test", "fields" : [{"name": "val", "type": "int", "default": 0}]}`)
	if err != nil {
		t.Errorf("Could not create codec %v", err)
	}
	testObject.Codec = codec

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testObject.Count++
		if r.Method == "POST" {
			switch r.URL.String() {
			case fmt.Sprintf(subjectVersions, subject), fmt.Sprintf(deleteSubject, subject):
				response := idResponse{id}
				str, _ := json.Marshal(response)
				fmt.Fprintf(w, string(str))
			}
		} else if r.Method == "GET" {
			switch r.URL.String() {
			case fmt.Sprintf(schemaByID, id):
				escapedSchema := strings.Replace(codec.Schema(), "\"", "\\\"", -1)
				fmt.Fprintf(w, `{"schema": "%s"}`, escapedSchema)
			case subjects:
				response := []string{subject}
				str, _ := json.Marshal(response)
				fmt.Fprintf(w, string(str))
			case fmt.Sprintf(subjectVersions, subject):
				response := []int{id}
				str, _ := json.Marshal(response)
				fmt.Fprintf(w, string(str))
			case fmt.Sprintf(subjectByVersion, subject, "1"), fmt.Sprintf(subjectByVersion, subject, "latest"):
				response := schemaVersionResponse{subject, 1, codec.Schema(), id}
				str, _ := json.Marshal(response)
				fmt.Fprintf(w, string(str))
			}
		} else if r.Method == "DELETE" {
			switch r.URL.String() {
			case fmt.Sprintf(deleteSubject, subject),
				fmt.Sprintf(subjectByVersion, subject, fmt.Sprintf("%d", 1)):
				fmt.Fprintf(w, "[1]")
			}
		}

	}))
	testObject.MockServer = server
	return testObject
}

func TestCachedSchemaRegistryClient_GetSchemaByID(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client, err := kafkaavro.NewCachedSchemaRegistryClient(mockServer.URL)
	if nil != err {
		t.Errorf("Error creating cached schema registry client: %s", err.Error())
	}
	client.GetSchemaByID(1)
	responseCodec, err := client.GetSchemaByID(1)
	if nil != err {
		t.Errorf("Error getting schema: %s", err.Error())
	}
	if responseCodec.Schema() != testObject.Codec.Schema() {
		t.Errorf("Schemas do not match. Expected: %s, got: %s", testObject.Codec.Schema(), responseCodec.Schema())
	}
	if testObject.Count > 1 {
		t.Errorf("Expected call count of 1, got %d", testObject.Count)
	}
}

func TestCachedSchemaRegistryClient_Subjects(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client, err := kafkaavro.NewCachedSchemaRegistryClient(mockServer.URL)
	if nil != err {
		t.Errorf("Error creating cached schema registry client: %s", err.Error())
	}
	subjects, err := client.Subjects()
	if nil != err {
		t.Errorf("Error getting subjects: %v", err)
	}
	if !containsStr(subjects, testObject.Subject) {
		t.Errorf("Could not find subject")
	}
}

func TestCachedSchemaRegistryClient_GetVersions(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client, err := kafkaavro.NewCachedSchemaRegistryClient(mockServer.URL)
	if nil != err {
		t.Errorf("Error creating cached schema registry client: %s", err.Error())
	}
	versions, err := client.Versions(testObject.Subject)
	if nil != err {
		t.Errorf("Error getting versions: %v", err)
	}
	if !containsInt(versions, testObject.ID) {
		t.Errorf("Could not find version")
	}
}

func TestCachedSchemaRegistryClient_GetSchemaByVersion(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client, err := kafkaavro.NewCachedSchemaRegistryClient(mockServer.URL)
	if nil != err {
		t.Errorf("Error creating cached schema registry client: %s", err.Error())
	}
	responseCodec, err := client.GetSchemaBySubject(testObject.Subject, 1)
	if nil != err {
		t.Errorf("Error getting schema versions: %v", err)
	}
	if responseCodec.Schema() != testObject.Codec.Schema() {
		t.Errorf("Schemas do not match. Expected: %s, got: %s", testObject.Codec.Schema(), responseCodec.Schema())
	}
}

func TestCachedSchemaRegistryClient_GetLatestSchema(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client, err := kafkaavro.NewCachedSchemaRegistryClient(mockServer.URL)
	if nil != err {
		t.Errorf("Error creating cached schema registry client: %s", err.Error())
	}
	responseCodec, err := client.GetLatestSchema(testObject.Subject)
	if nil != err {
		t.Errorf("Error getting latest schema: %v", err)
	}
	if responseCodec.Schema() != testObject.Codec.Schema() {
		t.Errorf("Schemas do not match. Expected: %s, got: %s", testObject.Codec.Schema(), responseCodec.Schema())
	}
}

func TestCachedSchemaRegistryClient_CreateSubject(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client, err := kafkaavro.NewCachedSchemaRegistryClient(mockServer.URL)
	if nil != err {
		t.Errorf("Error creating cached schema registry client: %s", err.Error())
	}
	id, err := client.RegisterNewSchema(testObject.Subject, testObject.Codec)
	if nil != err {
		t.Errorf("Error getting schema: %s", err.Error())
	}
	if id != testObject.ID {
		t.Errorf("Ids do not match. Expected: %d, got: %d", testObject.ID, id)
	}
	sameid, err := client.RegisterNewSchema(testObject.Subject, testObject.Codec)
	if nil != err {
		t.Errorf("Error getting schema: %s", err.Error())
	}
	if sameid != id {
		t.Errorf("Ids do not match. Expected: %d, got: %d", id, sameid)
	}
	if testObject.Count > 1 {
		t.Errorf("Expected call count of 1, got %d", testObject.Count)
	}
}

func TestCachedSchemaRegistryClient_IsSchemaRegistered(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client, err := kafkaavro.NewCachedSchemaRegistryClient(mockServer.URL)
	if nil != err {
		t.Errorf("Error creating cached schema registry client: %s", err.Error())
	}
	found, schema, err := client.IsSchemaRegistered(testObject.Subject, testObject.Codec)
	if nil != err {
		t.Errorf("Error getting schema id: %v", err)
	}
	if !found {
		t.Error("Error getting schema")
	}
	if schema.ID != testObject.ID {
		t.Errorf("Ids do not match. Expected: %d, got: %d", testObject.ID, schema.ID)
	}
}

func TestCachedSchemaRegistryClient_DeleteSubject(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client, err :=kafkaavro. NewCachedSchemaRegistryClient(mockServer.URL)
	if nil != err {
		t.Errorf("Error creating cached schema registry client: %s", err.Error())
	}
	_, err = client.DeleteSubject(testObject.Subject)
	if nil != err {
		t.Errorf("Error delete subject: %v", err)
	}
}

func containsStr(array []string, value string) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func containsInt(array []int, value int) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}
