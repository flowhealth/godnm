package dnm

import (
	"fmt"
	"github.com/crowdmob/goamz/dynamodb"
)

func Describe(name string, definitions func(ITable)) dynamodb.TableDescriptionT {
	table := makeTable(name)
	definitions(table)
	return table.TableDescriptionT
}

type ITable interface {
	Attr(name, typ string) *dynamodb.AttributeDefinitionT
	PrimaryKey() iPrimaryKey
	GlobalIndex(name string) iGlobalIndex
	LocalIndex(name string) iLocalIndex
	ProvisionedThroughput() iProvisionedThroughput
}

type iGlobalIndex interface {
	Hash(*dynamodb.AttributeDefinitionT)
	Range(*dynamodb.AttributeDefinitionT)
	ProvisionedThroughput() iProvisionedThroughput
	Projection() iProjection
}

type iLocalIndex interface {
	Hash(*dynamodb.AttributeDefinitionT)
	Range(*dynamodb.AttributeDefinitionT)
	Projection() iProjection
}

type tTable struct {
	dynamodb.TableDescriptionT
}

func makeTable(name string) *tTable {
	return &tTable{dynamodb.TableDescriptionT{TableName: name,
		AttributeDefinitions:   []dynamodb.AttributeDefinitionT{},
		KeySchema:              []dynamodb.KeySchemaT{},
		ProvisionedThroughput:  dynamodb.ProvisionedThroughputT{},
		GlobalSecondaryIndexes: []dynamodb.GlobalSecondaryIndexT{},
	}}
}

func (self *tTable) Attr(name, typ string) *dynamodb.AttributeDefinitionT {
	if !self.isUniqueAttrName(name) {
		panic(fmt.Sprintf("Incorrect table definition: duplicate attr name %s", name))
	}
	attr := dynamodb.AttributeDefinitionT{Name: name, Type: typ}
	self.AttributeDefinitions = append(self.AttributeDefinitions, attr)
	return &attr
}

func (self *tTable) PrimaryKey() iPrimaryKey {
	return makePrimaryKey(self)
}

func (self *tTable) isUniqueAttrName(name string) bool {
	for _, v := range self.AttributeDefinitions {
		if v.Name == name {
			return false
		}
	}
	return true
}

func (self *tTable) isGlobalIndexUniqueName(name string) bool {
	for _, v := range self.GlobalSecondaryIndexes {
		if v.IndexName == name {
			return false
		}
	}
	return true
}

func (self *tTable) addGlobalIndex(name string) *dynamodb.GlobalSecondaryIndexT {
	assertCorrectIndexName(name)
	if !self.isGlobalIndexUniqueName(name) {
		panic(fmt.Sprintf("Incorrect table definition: duplicate local index name %s", name))
	}
	idx := dynamodb.GlobalSecondaryIndexT{IndexName: name,
		KeySchema:             []dynamodb.KeySchemaT{},
		Projection:            dynamodb.ProjectionT{},
		ProvisionedThroughput: dynamodb.ProvisionedThroughputT{},
	}
	self.GlobalSecondaryIndexes = append(self.GlobalSecondaryIndexes, idx)
	insertedIdxPos := len(self.GlobalSecondaryIndexes) - 1
	idxPtr := &self.GlobalSecondaryIndexes[insertedIdxPos]
	return idxPtr
}

func (self *tTable) GlobalIndex(name string) iGlobalIndex {
	idx := self.addGlobalIndex(name)
	return makeGlobalIndex(idx)
}

func (self *tTable) isLocalIndexUniqueName(name string) bool {
	for _, v := range self.LocalSecondaryIndexes {
		if v.IndexName == name {
			return false
		}
	}
	return true
}

func (self *tTable) addLocalIndex(name string) *dynamodb.LocalSecondaryIndexT {
	assertCorrectIndexName(name)
	if !self.isLocalIndexUniqueName(name) {
		panic(fmt.Sprintf("Incorrect table definition: duplicate local index name %s", name))
	}
	idx := dynamodb.LocalSecondaryIndexT{IndexName: name,
		KeySchema: []dynamodb.KeySchemaT{},
		Projection: dynamodb.ProjectionT{
			NonKeyAttributes: []string{},
		},
	}
	self.LocalSecondaryIndexes = append(self.LocalSecondaryIndexes, idx)
	insertedIdxPos := len(self.LocalSecondaryIndexes) - 1
	idxPtr := &self.LocalSecondaryIndexes[insertedIdxPos]
	return idxPtr
}

func (self *tTable) LocalIndex(name string) iLocalIndex {
	idx := self.addLocalIndex(name)
	return makeLocalIndex(idx)
}

func (self *tTable) ProvisionedThroughput() iProvisionedThroughput {
	return makeProvisionedThroughput(&self.TableDescriptionT.ProvisionedThroughput)
}

func assertCorrectIndexName(name string) {
	namelen := len(name)
	conforms := namelen > 3 && namelen <= 255
	if !conforms {
		panic(fmt.Sprintf("Incorrect table definition: index name %s is illegal. Minimum length of 3. Maximum length of 255", name))
	}
}
