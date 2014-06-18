package dnm

import "github.com/flowhealth/goamz/dynamodb"

type tProjection struct {
	projection *dynamodb.ProjectionT
	keySchema  iKeySchema
}

func makeProjection(projection *dynamodb.ProjectionT, keySchema iKeySchema) iProjection {
	return &tProjection{projection, keySchema}
}

type iProjection interface {
	Include(...*dynamodb.AttributeDefinitionT)
	All()
	KeysOnly()
}

func (self *tProjection) KeysOnly() {
	self.projection.ProjectionType = ProjectionTypeKeysOnly
}

func (self *tProjection) All() {
	self.projection.ProjectionType = ProjectionTypeAll
}

func (self *tProjection) isUniqueNonKeyAttr(nonKeyAttr *dynamodb.AttributeDefinitionT) bool {
	for _, v := range self.projection.NonKeyAttributes {
		if v == nonKeyAttr.Name {
			return false
		}
	}
	return true
}

func (self *tProjection) isNonKeyAttr(nonKeyAttr *dynamodb.AttributeDefinitionT) bool {
	for _, v := range self.keySchema.Items() {
		if v.AttributeName == nonKeyAttr.Name {
			return false
		}
	}
	return true
}

func (self *tProjection) Include(attrs ...*dynamodb.AttributeDefinitionT) {
	if len(attrs)+len(self.keySchema.Items()) > ProjectionNonKeyAttrLimit {
		panic("Incorrect table definition: projection cant include more than 20 non-key attributes")
	}
	for _, v := range attrs {
		if !self.isNonKeyAttr(v) {
			panic("Incorrect table definition: projection cant include key attribute in non-key attr list")
		}
		if !self.isUniqueNonKeyAttr(v) {
			panic("Incorrect table definition: projection cant include duplicate key attribute in non-key attr list")
		}
		self.projection.NonKeyAttributes = append(self.projection.NonKeyAttributes, v.Name)
	}
	self.projection.ProjectionType = ProjectionTypeInclude
}
