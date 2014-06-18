package dnm

import "github.com/flowhealth/goamz/dynamodb"

type iPrimaryKey interface {
	Hash(IAttr)
	Range(IAttr)
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
	return &tIndex{"PK_IGNORE", "PK_IGNORE", makeTableKeySchema(table)}
}
