package dnm

import "github.com/flowhealth/goamz/dynamodb"

/**
GlobalSecondary Index
*/

type tGlobalIndexKeySchema struct {
	idx *dynamodb.GlobalSecondaryIndexT
}

func (self *tGlobalIndexKeySchema) Items() []dynamodb.KeySchemaT {
	return self.idx.KeySchema
}

func (self *tGlobalIndexKeySchema) Append(k dynamodb.KeySchemaT) {
	self.idx.KeySchema = append(self.idx.KeySchema, k)
}

func makeGlobalIndexKeySchema(idx *dynamodb.GlobalSecondaryIndexT) iKeySchema {
	return &tGlobalIndexKeySchema{idx}
}

type tGlobalIndex struct {
	gidef *dynamodb.GlobalSecondaryIndexT
	tIndex
}

func makeGlobalIndex(gidef *dynamodb.GlobalSecondaryIndexT) iGlobalIndex {
	schema := makeGlobalIndexKeySchema(gidef)
	idx := tIndex{schema}
	return &tGlobalIndex{gidef, idx}
}

func (self *tGlobalIndex) Projection() iProjection {
	schema := makeGlobalIndexKeySchema(self.gidef)
	return makeProjection(&self.gidef.Projection, schema)
}

func (self *tGlobalIndex) ProvisionedThroughput() iProvisionedThroughput {
	return makeProvisionedThroughput(&self.gidef.ProvisionedThroughput)
}
