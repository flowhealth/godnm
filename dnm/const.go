package dnm

import "github.com/flowhealth/goamz/dynamodb"

const (
	ProjectionNonKeyAttrLimit = 20
	ProjectionTypeAll         = "ALL"
	ProjectionTypeInclude     = "INCLUDE"
	ProjectionTypeKeysOnly    = "KEYS_ONLY"
	MaxIndexKeys              = 2
	String                    = dynamodb.TYPE_STRING
	Number                    = dynamodb.TYPE_NUMBER
	Binary                    = dynamodb.TYPE_BINARY
	KeyRange                  = "RANGE"
	KeyHash                   = "HASH"
)
