package dnm

import (
	"fmt"
	"github.com/flowhealth/goamz/dynamodb"
)

func Describe(name string, definitions func(ITable)) dynamodb.TableDescriptionT {
	table := makeTable(name)
	definitions(table)
	return table.TableDescriptionT
}

type ITable interface {
	KeyAttr(name, typ string) *tAttr
	NonKeyAttr(name, typ string) *tAttr
	PrimaryKey() iPrimaryKey
	GlobalIndex(name string) iGlobalIndex
	LocalIndex(name string) iLocalIndex
	ProvisionedThroughput() iProvisionedThroughput
}

type iGlobalIndex interface {
	Hash(IAttr)
	Range(IAttr)
	ProvisionedThroughput() iProvisionedThroughput
	Projection() iProjection
	Where(conds ...dynamodb.AttributeComparison) *dynamodb.Query
}

type iLocalIndex interface {
	Hash(IAttr)
	Range(IAttr)
	Projection() iProjection
	Where(conds ...dynamodb.AttributeComparison) *dynamodb.Query
}

type tTable struct {
	dynamodb.TableDescriptionT
	attrNames []string
	name      string
}

func makeTable(name string) *tTable {
	return &tTable{dynamodb.TableDescriptionT{TableName: name,
		AttributeDefinitions:   []dynamodb.AttributeDefinitionT{},
		KeySchema:              []dynamodb.KeySchemaT{},
		ProvisionedThroughput:  dynamodb.ProvisionedThroughputT{},
		GlobalSecondaryIndexes: []dynamodb.GlobalSecondaryIndexT{},
	}, []string{}, name}
}

func (self *tTable) KeyAttr(name, typ string) *tAttr {
	if !self.tryClaimAttrName(name) {
		panic(fmt.Sprintf("Incorrect table definition: duplicate attr name %s", name))
	}
	attr := dynamodb.AttributeDefinitionT{Name: name, Type: typ}
	self.AttributeDefinitions = append(self.AttributeDefinitions, attr)
	return makeAttr(&attr)
}

func (self *tTable) NonKeyAttr(name, typ string) *tAttr {
	if !self.tryClaimAttrName(name) {
		panic(fmt.Sprintf("Incorrect table definition: duplicate attr name %s", name))
	}
	attr := dynamodb.AttributeDefinitionT{Name: name, Type: typ}
	return makeAttr(&attr)
}

func (self *tTable) PrimaryKey() iPrimaryKey {
	return makePrimaryKey(self)
}

func (self *tTable) tryClaimAttrName(name string) bool {
	for _, v := range self.attrNames {
		if v == name {
			return false
		}
	}
	self.attrNames = append(self.attrNames, name)
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
	return makeGlobalIndex(self.name, idx)
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
	return makeLocalIndex(self.name, idx)
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
