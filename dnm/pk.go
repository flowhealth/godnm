package dnm

import "github.com/crowdmob/goamz/dynamodb"

type iPrimaryKey interface {
	Hash(*dynamodb.AttributeDefinitionT)
	Range(*dynamodb.AttributeDefinitionT)
}

type tPrimaryKey struct {
	table *tTable
}

type TTableKeySchema struct {
	table *tTable
}

func (self *TTableKeySchema) Items() []dynamodb.KeySchemaT {
	return self.table.KeySchema
}

func (self *TTableKeySchema) Append(k dynamodb.KeySchemaT) {
	self.table.KeySchema = append(self.table.KeySchema, k)
}

func makeTableKeySchema(table *tTable) iKeySchema {
	return &TTableKeySchema{table}
}

func makePrimaryKey(table *tTable) iPrimaryKey {
	return &tIndex{makeTableKeySchema(table)}
}
