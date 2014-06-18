package dnm

import (
	"fmt"
	"github.com/flowhealth/goamz/dynamodb"
)

type iKeySchema interface {
	Items() []dynamodb.KeySchemaT
	Append(dynamodb.KeySchemaT)
}

/////

type tIndex struct {
	name      string
	tableName string
	keySchema iKeySchema
}

func (self *tIndex) hasHashKey() bool {
	return self.hasKey(KeyHash)
}

func (self *tIndex) hasRangeKey() bool {
	return self.hasKey(KeyRange)
}

func (self *tIndex) hasKey(typ string) bool {
	for _, v := range self.keySchema.Items() {
		if v.KeyType == typ {
			return true
		}
	}
	return false
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

func (self *tIndex) Hash(attr IAttr) {
	self.tryAddKey(KeyHash, attr.Def())
}

func (self *tIndex) Range(attr IAttr) {
	self.tryAddKey(KeyRange, attr.Def())
}

func (self *tIndex) Where(conds ...dynamodb.AttributeComparison) *dynamodb.Query {
	q := dynamodb.NewQueryFor(self.tableName)
	q.AddKeyConditions(conds)
	q.AddIndex(self.name)
	return q
}
