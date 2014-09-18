package dnm

import (
	"time"

	"github.com/flowhealth/goamz/dynamodb"
)

type IIndex interface {
	Where(...dynamodb.AttributeComparison) *dynamodb.Query
}

type IAttr interface {
	Equals(val string) dynamodb.AttributeComparison
	Is(val ...string) dynamodb.Attribute
	Def() *dynamodb.AttributeDefinitionT
	From(map[string]*dynamodb.Attribute) string
}
type tAttr struct {
	def                   *dynamodb.AttributeDefinitionT
	updateAttrTypeInTable func(string)
}

func (self *tAttr) From(attrMap map[string]*dynamodb.Attribute) string {
	if val, ok := attrMap[self.def.Name]; ok {
		return val.Value
	} else {
		return ""
	}
}

func (self *tAttr) Def() *dynamodb.AttributeDefinitionT {
	return self.def
}

func (self *tAttr) Is(vals ...string) dynamodb.Attribute {
	isNumSet := (self.def.Type == dynamodb.TYPE_NUMBER_SET)
	isBinSet := (self.def.Type == dynamodb.TYPE_BINARY_SET)
	isStrSet := (self.def.Type == dynamodb.TYPE_STRING_SET)
	isSet := isNumSet || isBinSet || isStrSet
	if isSet {
		return dynamodb.Attribute{
			Type: self.def.Type,
			Name: self.def.Name, SetValues: vals,
		}
	} else {
		if len(vals) == 1 {
			if vals[0] == "" {
				panic("Invalid empty value is not allowed")
			}
			return dynamodb.Attribute{
				Type: self.def.Type,
				Name: self.def.Name, Value: vals[0],
			}
		} else {
			panic("Invalid set of values")
		}
	}
}
func (self *tAttr) Equals(val string) dynamodb.AttributeComparison {
	return dynamodb.AttributeComparison{self.def.Name,
		dynamodb.COMPARISON_EQUAL,
		[]dynamodb.Attribute{self.Is(val)},
	}
}

func (self *tAttr) NotEquals(val string) dynamodb.AttributeComparison {
	return dynamodb.AttributeComparison{self.def.Name,
		dynamodb.COMPARISON_NOT_EQUAL,
		[]dynamodb.Attribute{self.Is(val)},
	}
}

func makeAttr(attr *dynamodb.AttributeDefinitionT, typeSetter func(string)) *tAttr {
	return &tAttr{attr, typeSetter}
}

/*
 bool attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsBool() tBoolAttr {
	self.def.Type = Number
	self.updateAttrTypeInTable(Number)
	return tBoolAttr{*self}
}

// serializer

type tBoolAttr struct {
	tAttr
}

func (self *tBoolAttr) Is(val bool) dynamodb.Attribute {
	return self.tAttr.Is(FromBool(val))
}

func (self *tBoolAttr) From(attrMap map[string]*dynamodb.Attribute) (bool, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		return ToBool(self.def.Name, val)
	} else {
		return false, AttrNotFoundErr
	}
}

/*
 []byte attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsBinary() tBinaryAttr {
	self.def.Type = Binary
	self.updateAttrTypeInTable(Binary)
	return tBinaryAttr{*self}
}

// serializer

type tBinaryAttr struct {
	tAttr
}

func (self *tBinaryAttr) Is(val []byte) dynamodb.Attribute {
	return self.tAttr.Is(FromBinary(val))
}

func (self *tBinaryAttr) From(attrMap map[string]*dynamodb.Attribute) ([]byte, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		return ToBinary(self.def.Name, val)
	} else {
		return nil, AttrNotFoundErr
	}
}

/*
 time.Time attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsTimeTime() tTimeTimeAttr {
	self.def.Type = Number
	self.updateAttrTypeInTable(Number)
	return tTimeTimeAttr{*self}
}

// serializer

type tTimeTimeAttr struct {
	tAttr
}

func (self *tTimeTimeAttr) Is(val time.Time) dynamodb.Attribute {
	return self.tAttr.Is(FromTimeTime(val))
}

func (self *tTimeTimeAttr) From(attrMap map[string]*dynamodb.Attribute) (*time.Time, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		if t, e := ToTimeTime(self.def.Name, val); e != nil {
			return nil, e
		} else {
			return &t, nil
		}
	} else {
		return nil, AttrNotFoundErr
	}
}

/*
 string attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsString() tStringAttr {
	self.def.Type = String
	self.updateAttrTypeInTable(String)
	return tStringAttr{*self}
}

// serializer

type tStringAttr struct {
	tAttr
}

func (self *tStringAttr) Is(val string) dynamodb.Attribute {
	return self.tAttr.Is(FromString(val))
}

func (self *tStringAttr) From(attrMap map[string]*dynamodb.Attribute) (string, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		return ToString(val), nil
	} else {
		return "", AttrNotFoundErr
	}
}

/*
 float32 attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsFloat32() tFloat32Attr {
	self.def.Type = Number
	self.updateAttrTypeInTable(Number)
	return tFloat32Attr{*self}
}

// serializer

type tFloat32Attr struct {
	tAttr
}

func (self *tFloat32Attr) Is(val float32) dynamodb.Attribute {
	return self.tAttr.Is(FromFloat32(val))
}

func (self *tFloat32Attr) From(attrMap map[string]*dynamodb.Attribute) (float32, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		return ToFloat32(self.def.Name, val)
	} else {
		return 0, AttrNotFoundErr
	}
}

/*
 float64 attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsFloat64() tFloat64Attr {
	self.def.Type = Number
	self.updateAttrTypeInTable(Number)
	return tFloat64Attr{*self}
}

// serializer

type tFloat64Attr struct {
	tAttr
}

func (self *tFloat64Attr) Is(val float64) dynamodb.Attribute {
	return self.tAttr.Is(FromFloat64(val))
}

func (self *tFloat64Attr) From(attrMap map[string]*dynamodb.Attribute) (float64, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		return ToFloat64(self.def.Name, val)
	} else {
		return 0, AttrNotFoundErr
	}
}

/*
 int attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsInt() tIntAttr {
	self.def.Type = Number
	self.updateAttrTypeInTable(Number)
	return tIntAttr{*self}
}

// serializer

type tIntAttr struct {
	tAttr
}

func (self *tIntAttr) Is(val int) dynamodb.Attribute {
	return self.tAttr.Is(FromInt(val))
}

func (self *tIntAttr) From(attrMap map[string]*dynamodb.Attribute) (int, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		return ToInt(self.def.Name, val)
	} else {
		return 0, AttrNotFoundErr
	}
}

/*
 int32 attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsInt32() tInt32Attr {
	self.def.Type = Number
	self.updateAttrTypeInTable(Number)
	return tInt32Attr{*self}
}

// serializer

type tInt32Attr struct {
	tAttr
}

func (self *tInt32Attr) Is(val int32) dynamodb.Attribute {
	return self.tAttr.Is(FromInt32(val))
}

func (self *tInt32Attr) From(attrMap map[string]*dynamodb.Attribute) (int32, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		return ToInt32(self.def.Name, val)
	} else {
		return 0, AttrNotFoundErr
	}
}

/*
 int64 attribute serialization/deserialization
*/

// convenience method

func (self *tAttr) AsInt64() tInt64Attr {
	self.def.Type = Number
	self.updateAttrTypeInTable(Number)
	return tInt64Attr{*self}
}

// serializer

type tInt64Attr struct {
	tAttr
}

func (self *tInt64Attr) Is(val int64) dynamodb.Attribute {
	return self.tAttr.Is(FromInt64(val))
}

func (self *tInt64Attr) From(attrMap map[string]*dynamodb.Attribute) (int64, error) {
	if val := self.tAttr.From(attrMap); val != "" {
		return ToInt64(self.def.Name, val)
	} else {
		return 0, AttrNotFoundErr
	}
}
