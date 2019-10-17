package kafkaavro

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

type NativeDecoder interface {
	FromNative(interface{}) error
}

var nativeDecoderType = reflect.TypeOf((*NativeDecoder)(nil)).Elem()

func DecodeRecordFromNative(src interface{}, dst interface{}) error {
	record, ok := src.(map[string]interface{})
	if !ok {
		return errors.New("record type assertion failed")
	}
	structValue := reflect.ValueOf(dst).Elem()
	for key, data := range record {
		dstField := structValue.FieldByNameFunc(func(name string) bool {
			return strings.ToLower(key) == strings.ToLower(name)
		})

		if !dstField.IsValid() {
			continue
		}

		err := valueFromNative(data, dstField)
		if err != nil {
			return errors.Wrapf(err, "field %s", key)
		}
	}
	return nil
}

func decodeSliceFromNative(src interface{}, dstVal reflect.Value) error {
	if reflect.TypeOf(src).Kind() != reflect.Slice {
		return errors.New("source type is not slice")
	}
	srcVal := reflect.ValueOf(src)
	dstVal.Set(reflect.MakeSlice(dstVal.Type(), srcVal.Len(), srcVal.Len()))
	for i := 0; i < srcVal.Len(); i++ {
		err := valueFromNative(srcVal.Index(i).Interface(), dstVal.Index(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func valueFromNative(src interface{}, value reflect.Value) error {
	sv := reflect.ValueOf(src)

	valueTypeKind := value.Type().Kind()

	if valueTypeKind == reflect.Slice {
		return decodeSliceFromNative(src, value)
	}

	// Check if value is a pointer and implements NativeDecoder
	if valueTypeKind == reflect.Ptr && value.Type().Implements(nativeDecoderType) {
		if value.IsNil() {
			value.Set(reflect.New(value.Type().Elem()))
		}
		err := value.Interface().(NativeDecoder).FromNative(src)
		if err != nil {
			return err
		}
		return nil
	}

	// Check if value is a pointer to a struct
	if valueTypeKind == reflect.Ptr && value.Type().Elem().Kind() == reflect.Struct {
		if src == nil && value.IsNil() {
			return nil
		}
		if src == nil && !value.IsNil() {
			value.Set(reflect.Zero(value.Type()))
			return nil
		}
		if value.IsNil() {
			value.Set(reflect.New(value.Type().Elem()))
		}
		// Set fields for optional struct
		// That means source is a map with single field corresponding with record type
		srcMap, ok := src.(map[string]interface{})
		if !ok || len(srcMap) != 1 {
			return errors.Errorf("missing record %v", src)
		}
		for _, srcMapVal := range srcMap {
			return DecodeRecordFromNative(srcMapVal, value.Interface())
		}
		return nil
	}
	// Check if value is not a pointer and implements NativeDecoder
	if valueTypeKind != reflect.Ptr && reflect.PtrTo(value.Type()).Implements(nativeDecoderType) {
		err := value.Addr().Interface().(NativeDecoder).FromNative(src)
		if err != nil {
			return err
		}
		return nil
	}

	// Check if value is a struct
	if valueTypeKind == reflect.Struct && sv.Kind() == reflect.Map {
		return DecodeRecordFromNative(src, value.Addr().Interface())
	}

	if sv.IsValid() && sv.Type().AssignableTo(value.Type()) {
		value.Set(sv)
		return nil
	}

	if value.Kind() == sv.Kind() && sv.Type().ConvertibleTo(value.Type()) {
		value.Set(sv.Convert(value.Type()))
		return nil
	}

	return errors.Errorf("%s type is not %s", reflect.TypeOf(src), value.Type())
}
