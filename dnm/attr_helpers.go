package dnm

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/flowhealth/goamz/dynamodb"
)

func MakeAttrNotFoundErr(attr string) error {
	return fmt.Errorf("DeSerialization error: attribute %s not found")
}

func MakeAttrInvalidErr(attr, value string) error {
	return fmt.Errorf("DeSerialization error: attribute %s has unexpected value: %s", attr, value)
}

/**
Bool
*/

const BoolAttrType = dynamodb.TYPE_NUMBER

const (
	DynamoBoolTrue  = "1"
	DynamoBoolFalse = "0"
)

func FromBool(value bool) string {
	var converted string
	if value == true {
		converted = DynamoBoolTrue
	} else {
		converted = DynamoBoolFalse
	}
	return converted
}

func ToBool(name, val string) (bool, error) {
	if val == DynamoBoolTrue {
		return true, nil
	} else if val == DynamoBoolFalse {
		return false, nil
	} else {
		return false, MakeAttrInvalidErr(name, val)
	}
}

func MakeBoolAttr(name string, value bool) dynamodb.Attribute {
	return *dynamodb.NewNumericAttribute(name, FromBool(value))
}

func GetBoolAttr(name string, attrs map[string]*dynamodb.Attribute) (bool, error) {
	if val, ok := attrs[name]; !ok {
		return false, MakeAttrNotFoundErr(name)
	} else {
		return ToBool(name, val.Value)
	}
}

/**
Int
*/

const IntAttrType = dynamodb.TYPE_NUMBER

func FromInt(val int) string {
	return strconv.FormatInt(int64(val), 10)
}

func ToInt(name, val string) (int, error) {
	if v64, err := strconv.ParseInt(val, 10, 64); err != nil {
		return 0, MakeAttrInvalidErr(name, val)
	} else {
		return int(v64), nil
	}
}

func MakeIntAttr(name string, value int) dynamodb.Attribute {
	return *dynamodb.NewNumericAttribute(name, FromInt(value))
}

func GetIntAttr(name string, attrs map[string]*dynamodb.Attribute) (v int, err error) {
	if val, ok := attrs[name]; !ok {
		return 0, MakeAttrNotFoundErr(name)
	} else {
		return ToInt(name, val.Value)
	}
}

/**
Int64
*/

const Int64AttrType = dynamodb.TYPE_NUMBER

func FromInt64(val int64) string {
	return strconv.FormatInt(int64(val), 10)
}

func ToInt64(name, val string) (int64, error) {
	if v64, err := strconv.ParseInt(val, 10, 64); err != nil {
		return 0, MakeAttrInvalidErr(name, val)
	} else {
		return v64, nil
	}
}

func MakeInt64Attr(name string, value int64) dynamodb.Attribute {
	return *dynamodb.NewNumericAttribute(name, FromInt64(value))
}

func GetInt64Attr(name string, attrs map[string]*dynamodb.Attribute) (v int64, err error) {
	if val, ok := attrs[name]; !ok {
		return 0, MakeAttrNotFoundErr(name)
	} else {
		return ToInt64(name, val.Value)
	}
}

/**
Int32
*/

const Int32AttrType = dynamodb.TYPE_NUMBER

func FromInt32(val int32) string {
	return strconv.FormatInt(int64(val), 10)
}

func ToInt32(name, val string) (int32, error) {
	if v64, err := strconv.ParseInt(val, 10, 32); err != nil {
		return 0, MakeAttrInvalidErr(name, val)
	} else {
		return int32(v64), nil
	}
}

func MakeInt32Attr(name string, value int32) dynamodb.Attribute {
	return *dynamodb.NewNumericAttribute(name, FromInt32(value))
}

func GetInt32Attr(name string, attrs map[string]*dynamodb.Attribute) (v int32, err error) {
	if val, ok := attrs[name]; !ok {
		return 0, MakeAttrNotFoundErr(name)
	} else {
		return ToInt32(name, val.Value)
	}
}

/**
Float32
*/

const Float32AttrType = dynamodb.TYPE_NUMBER

func FromFloat32(val float32) string {
	return strconv.FormatFloat(float64(val), 'f', -1, 32)
}

func ToFloat32(name, val string) (float32, error) {
	if v64, err := strconv.ParseFloat(val, 32); err != nil {
		return float32(0), MakeAttrInvalidErr(name, val)
	} else {
		return float32(v64), nil
	}
}

func MakeFloat32Attr(name string, val float32) dynamodb.Attribute {
	return *dynamodb.NewNumericAttribute(name, FromFloat32(val))
}

func GetFloat32Attr(name string, attrs map[string]*dynamodb.Attribute) (v float32, err error) {
	if val, ok := attrs[name]; !ok {
		err = MakeAttrNotFoundErr(name)
		return
	} else {
		return ToFloat32(name, val.Value)
	}
}

/**
Float64
*/

const Float64AttrType = dynamodb.TYPE_NUMBER

func FromFloat64(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

func ToFloat64(name, val string) (v64 float64, err error) {
	if v64, err = strconv.ParseFloat(val, 64); err != nil {
		err = MakeAttrInvalidErr(name, val)
	} else {
		return v64, nil
	}
	return
}

func MakeFloat64Attr(name string, val float64) dynamodb.Attribute {
	return *dynamodb.NewNumericAttribute(name, FromFloat64(val))
}

func GetFloat64Attr(name string, attrs map[string]*dynamodb.Attribute) (v float64, err error) {
	if val, ok := attrs[name]; !ok {
		err = MakeAttrNotFoundErr(name)
		return
	} else {
		return ToFloat64(name, val.Value)
	}
}

/**
Binary
*/

const BinaryAttrType = dynamodb.TYPE_STRING

func FromBinary(val []byte) string {
	return base64.StdEncoding.EncodeToString(val)
}

func ToBinary(name, val string) ([]byte, error) {
	if binVal, err := base64.StdEncoding.DecodeString(val); err != nil {
		return nil, err
	} else {
		return binVal, nil
	}
}

func MakeBinaryAttr(name string, value []byte) dynamodb.Attribute {
	return *dynamodb.NewBinaryAttribute(name, FromBinary(value))
}

func GetBinaryAttr(name string, attrs map[string]*dynamodb.Attribute) ([]byte, error) {
	if val, ok := attrs[name]; !ok {
		return nil, MakeAttrNotFoundErr(name)
	} else {
		if val.Value == NullString {
			return []byte{}, nil
		} else {
			return ToBinary(name, val.Value)
		}
	}
}

/**
time.Time
*/

const TimeTimeAttrType = dynamodb.TYPE_NUMBER

func FromTimeTime(value time.Time) string {
	return strconv.FormatInt(value.Unix(), 10)
}

func ToTimeTime(name, value string) (time.Time, error) {
	if timestamp, err := strconv.ParseInt(value, 10, 64); err != nil {
		return time.Time{}, MakeAttrInvalidErr(name, value)
	} else {
		return time.Unix(timestamp, 0), nil
	}
}

func MakeTimeTimeAttr(name string, value time.Time) dynamodb.Attribute {
	return *dynamodb.NewNumericAttribute(name, FromTimeTime(value))
}

func GetTimeTimeAttr(name string, attrs map[string]*dynamodb.Attribute) (time.Time, error) {
	if val, ok := attrs[name]; !ok {
		return time.Time{}, MakeAttrNotFoundErr(name)
	} else {
		return ToTimeTime(name, val.Value)
	}
}

/**
time.Time in nanoseconds
*/

const TimeTimeNanoAttrType = dynamodb.TYPE_NUMBER

func FromTimeTimeNano(value time.Time) string {
	return strconv.FormatInt(value.UnixNano(), 10)
}

func ToTimeTimeNano(name, value string) (time.Time, error) {
	if timestamp, err := strconv.ParseInt(value, 10, 64); err != nil {
		return time.Time{}, MakeAttrInvalidErr(name, value)
	} else {
		return time.Unix(timestamp, 0), nil
	}
}

func MakeTimeTimeNanoAttr(name string, value time.Time) dynamodb.Attribute {
	return *dynamodb.NewNumericAttribute(name, FromTimeTimeNano(value))
}

func GetTimeTimeNanoAttr(name string, attrs map[string]*dynamodb.Attribute) (time.Time, error) {
	if val, ok := attrs[name]; !ok {
		return time.Time{}, MakeAttrNotFoundErr(name)
	} else {
		return ToTimeTimeNano(name, val.Value)
	}
}

/**
String
*/

const (
	NullString     = "NULL"
	StringAttrType = dynamodb.TYPE_STRING
)

func FromString(val string) string {
	if val == NullString {
		return ""
	} else {
		return val
	}
}

func ToString(val string) string {
	if val == "" {
		return NullString
	} else {
		return val
	}
}

func GetStringAttr(name string, attrs map[string]*dynamodb.Attribute) (string, error) {
	if val, ok := attrs[name]; !ok {
		return "", MakeAttrNotFoundErr(name)
	} else {
		return FromString(val.Value), nil
	}
}

func MakeStringAttr(name string, value string) dynamodb.Attribute {
	return *dynamodb.NewStringAttribute(name, ToString(value))
}
