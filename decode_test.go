package kafkaavro_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	kafkaavro "github.com/mycujoo/go-kafka-avro"
)

func TestDecodeFromNative(t *testing.T) {
	dt := time.Date(2018, 01, 01, 0, 0, 0, 0, time.UTC)

	type NestedStruct struct {
		NestedField string
	}

	type withString struct {
		Field string
	}
	type withPtr struct {
		Field *NestedStruct
	}
	type withStruct struct {
		Field NestedStruct
	}
	type withSlice struct {
		Field []int
	}
	type withOptionalString struct {
		Field kafkaavro.OptionalString
	}
	type withPtrOptionalString struct {
		Field *kafkaavro.OptionalString
	}
	type withOptionalInt struct {
		Field kafkaavro.OptionalInt
	}
	type withOptionalDay struct {
		Field kafkaavro.OptionalDay
	}
	type withUInt8 struct {
		Field uint8
	}

	val4 := kafkaavro.NewOptionalString("val4", true)

	testCases := map[string]struct {
		src      interface{}
		dst      interface{}
		expected interface{}
	}{
		"string field": {
			src: map[string]interface{}{
				"field": "val1",
			},
			dst: &withString{},
			expected: &withString{
				Field: "val1",
			},
		},
		"struct ptr to nil": {
			src: map[string]interface{}{
				"field": map[string]interface{}{
					"nestedRecord": map[string]interface{}{
						"nestedField": "val2",
					},
				},
			},
			dst: &withPtr{},
			expected: &withPtr{
				Field: &NestedStruct{
					NestedField: "val2",
				},
			},
		},
		"struct ptr to not nil": {
			src: map[string]interface{}{
				"field": map[string]interface{}{
					"nestedRecord": map[string]interface{}{
						"nestedField": "val3",
					},
				},
			},
			dst: &withPtr{
				Field: &NestedStruct{
					NestedField: "val2",
				},
			},
			expected: &withPtr{
				Field: &NestedStruct{
					NestedField: "val3",
				},
			},
		},
		"struct ptr nil to not nil": {
			src: map[string]interface{}{
				"field": nil,
			},
			dst: &withPtr{
				Field: &NestedStruct{
					NestedField: "val2",
				},
			},
			expected: &withPtr{
				Field: nil,
			},
		},
		"struct ptr nil to nil": {
			src: map[string]interface{}{
				"field": nil,
			},
			dst: &withPtr{},
			expected: &withPtr{
				Field: nil,
			},
		},
		"struct": {
			src: map[string]interface{}{
				"field": map[string]interface{}{
					"nestedField": "val3",
				},
			},
			dst: &withStruct{},
			expected: &withStruct{
				Field: NestedStruct{
					NestedField: "val3",
				},
			},
		},
		"slice": {
			src: map[string]interface{}{
				"field": []int{1, 2, 3, 4},
			},
			dst: &withSlice{},
			expected: &withSlice{
				Field: []int{1, 2, 3, 4},
			},
		},
		"nil optional string": {
			src: map[string]interface{}{
				"field": nil,
			},
			dst: &withOptionalString{},
			expected: &withOptionalString{
				Field: kafkaavro.NewOptionalString("", false),
			},
		},
		"not nil optional string": {
			src: map[string]interface{}{
				"field": map[string]interface{}{"string": "val4"},
			},
			dst: &withOptionalString{},
			expected: &withOptionalString{
				Field: val4,
			},
		},
		"not nil ptr to optional string": {
			src: map[string]interface{}{
				"field": map[string]interface{}{"string": "val4"},
			},
			dst: &withPtrOptionalString{},
			expected: &withPtrOptionalString{
				Field: &val4,
			},
		},
		"extra fields ignored": {
			src: map[string]interface{}{
				"field":      "test",
				"extraField": "extra",
			},
			dst: &withString{},
			expected: &withString{
				Field: "test",
			},
		},
		"nil optional int": {
			src: map[string]interface{}{
				"field": nil,
			},
			dst: &withOptionalInt{},
			expected: &withOptionalInt{
				Field: kafkaavro.NewOptionalInt(0, false),
			},
		},
		"not nil optional int": {
			src: map[string]interface{}{
				"field": map[string]interface{}{"int": int32(123)},
			},
			dst: &withOptionalInt{},
			expected: &withOptionalInt{
				Field: kafkaavro.NewOptionalInt(123, true),
			},
		},
		"not nil optional day": {
			src: map[string]interface{}{
				"field": map[string]interface{}{"int.date": dt},
			},
			dst: &withOptionalDay{},
			expected: &withOptionalDay{
				Field: kafkaavro.NewOptionalDay(dt, true),
			},
		},
		"convert byte": {
			src: map[string]interface{}{
				"field": byte(32),
			},
			dst: &withUInt8{},
			expected: &withUInt8{
				Field: 32,
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := kafkaavro.DecodeRecordFromNative(tc.src, tc.dst)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, tc.dst)
		})
	}
}
