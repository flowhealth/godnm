package dnm

import "github.com/crowdmob/goamz/dynamodb"

/**
LocalSecondary Index
*/

type TLocalIndexKeySchemaProxy struct {
	idx *dynamodb.LocalSecondaryIndexT
}

func (self *TLocalIndexKeySchemaProxy) Items() []dynamodb.KeySchemaT {
	return self.idx.KeySchema
}

func (self *TLocalIndexKeySchemaProxy) Append(k dynamodb.KeySchemaT) {
	self.idx.KeySchema = append(self.idx.KeySchema, k)
}

func makeLocalIndexKeySchemaProxy(idx *dynamodb.LocalSecondaryIndexT) iKeySchema {
	return &TLocalIndexKeySchemaProxy{idx}
}

type tLocalIndex struct {
	tIndex
	lidef          *dynamodb.LocalSecondaryIndexT
	keySchemaProxy iKeySchema
}

func makeLocalIndex(lidef *dynamodb.LocalSecondaryIndexT) iLocalIndex {
	schema := makeLocalIndexKeySchemaProxy(lidef)
	idx := tIndex{schema}
	return &tLocalIndex{idx, lidef, schema}
}

func (self *tLocalIndex) Projection() iProjection {
	return makeProjection(&self.lidef.Projection, self.keySchemaProxy)
}
