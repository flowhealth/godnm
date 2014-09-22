package dnm

import (
	"fmt"

	"github.com/flowhealth/goamz/dynamodb"
)

type iKeySchema interface {
	Items() []dynamodb.KeySchemaT
	Append(dynamodb.KeySchemaT)
}

type IKeyFactory interface {
	Key(dynamodb.Attribute, ...dynamodb.Attribute) dynamodb.Key
}

/////

type tIndex struct {
	name      string
	tableName string
	keySchema iKeySchema
}

func (self *tIndex) hasHashKey() bool {
	return self.attrNameByKeyType(KeyHash) != ""
}

func (self *tIndex) hasRangeKey() bool {
	return self.attrNameByKeyType(KeyRange) != ""
}

func (self *tIndex) attrNameByKeyType(typ string) string {
	for _, v := range self.keySchema.Items() {
		if v.KeyType == typ {
			return v.AttributeName
		}
	}
	return ""
}

func (self *tIndex) canAddKey(typ string) bool {
	if len(self.keySchema.Items()) == MaxIndexKeys {
		return false
	}
	for _, v := range self.keySchema.Items() {
		if v.KeyType == typ {
			return false
		}
	}
	return true
}

func validIndexType(typ string) bool {
	return typ == Number || typ == String || typ == Binary
}

func (self *tIndex) tryAddKey(typ string, attr *dynamodb.AttributeDefinitionT) {
	if !self.canAddKey(typ) {
		panic(fmt.Sprintf("Incorrect table definition: duplicate index key %s attribute", typ))
	}
	if !validIndexType(attr.Type) {
		panic(fmt.Sprintf("Incorrect table definition: unsupported index key %s attribute type", typ))
	}
	k := dynamodb.KeySchemaT{AttributeName: attr.Name, KeyType: typ}
	self.keySchema.Append(k)
}

func (self *tIndex) Hash(attr AttributeDefinitionProvider) {
	self.tryAddKey(KeyHash, attr.Def())
}

func (self *tIndex) Range(attr AttributeDefinitionProvider) {
	self.tryAddKey(KeyRange, attr.Def())
}

func (self *tIndex) Where(conds ...dynamodb.AttributeComparison) *dynamodb.Query {
	q := dynamodb.NewQueryFor(self.tableName)
	q.AddKeyConditions(conds)
	// index name can be empty if it's an index for a primary key
	if self.name != "" {
		q.AddIndex(self.name)
	}
	return q
}

func (self *tIndex) Factory() IKeyFactory {
	return self
}

func (self *tIndex) Key(hash dynamodb.Attribute, maybeRange ...dynamodb.Attribute) dynamodb.Key {
	expectedName := self.attrNameByKeyType(KeyHash)
	if expectedName != hash.Name {
		panic(fmt.Sprintf("Illegal key, hash key attribute name is incorrect, expected: %s, got %s", expectedName, hash.Name))
	}
	if len(maybeRange) == 1 {
		if self.hasRangeKey() {
			rang := maybeRange[0]
			expectedName := self.attrNameByKeyType(KeyRange)
			if expectedName == rang.Name {
				return dynamodb.Key{HashKey: hash.Value, RangeKey: rang.Value}
			} else {
				panic(fmt.Sprintf("Illegal key, range key attribute name is incorrect, expected: %s, got %s", expectedName, rang.Name))
			}
		} else {
			panic("Illegal key, range wasnt defined for this index")
		}
	} else if len(maybeRange) > 1 {
		panic("Illegal key, cant have multiple range attributes")
	} else {
		if self.hasRangeKey() {
			panic("Illegal key, range is required but want specified")
		}
		return dynamodb.Key{HashKey: hash.Value}
	}
}
