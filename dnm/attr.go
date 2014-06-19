package dnm

import "github.com/flowhealth/goamz/dynamodb"

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
	def *dynamodb.AttributeDefinitionT
}

func (self *tAttr) From(attrMap map[string]*dynamodb.Attribute) string {
	if val, ok := attrMap[self.def.Name]; ok {
		return val.Value
	} else {
		panic("Unable to get attribute value")
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

func makeAttr(attr *dynamodb.AttributeDefinitionT) *tAttr {
	return &tAttr{attr}
}
