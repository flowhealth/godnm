package dnm

import (
	"fmt"
	"github.com/flowhealth/commons/fhlog"
	"github.com/flowhealth/goamz/aws"
	"github.com/flowhealth/goamz/dynamodb"
	"github.com/flowhealth/goannoying"
	"github.com/flowhealth/gocontract/contract"
	log "github.com/flowhealth/logrus"
	"strings"
	"time"
)

const (
	TableStatusActive                   = "ACTIVE"
	TableStatusCreating                 = "CREATING"
	DefaultTableCreateCheckTimeout      = "60s"
	DefaultTableCreateCheckPollInterval = "5s"
	DefaultReadCapacity                 = 1
	DefaultWriteCapacity                = 1
	ActionAttributeUpdate               = "PUT"
	ConditionalDynamoError              = "ConditionalCheckFailedException"
)

var (
	DefaultRegion = aws.USWest2
)

type IStore interface {
	Get(key *dynamodb.Key) (map[string]*dynamodb.Attribute, *TError)
	Find(query *dynamodb.Query) ([]map[string]*dynamodb.Attribute, *TError)
	Save(...dynamodb.Attribute) *TError
	SaveConditional(attrs []dynamodb.Attribute, expected []dynamodb.Attribute) *TError
	SaveConditionalWithConditionExpression(attrs []dynamodb.Attribute, condition *dynamodb.ConditionExpression) *TError
	DeleteAttributesWithUpdateExpression(key *dynamodb.Key, returnValues string, attrs ...dynamodb.UpdateExpressionAttribute) (map[string]*dynamodb.Attribute, *TError)
	ModifyAttributesWithUpdateExpression(key *dynamodb.Key, condition *dynamodb.ConditionExpression, actions []string, returnValues string, attrs ...dynamodb.UpdateExpressionAttribute) (map[string]*dynamodb.Attribute, *TError)
	UpdateWithUpdateExpression(key *dynamodb.Key, returnValues string, attrs ...dynamodb.UpdateExpressionAttribute) (map[string]*dynamodb.Attribute, *TError)
	UpdateConditionalWithUpdateExpression(key *dynamodb.Key, condition *dynamodb.ConditionExpression, returnValues string, attrs ...dynamodb.UpdateExpressionAttribute) (map[string]*dynamodb.Attribute, *TError)
	Update(key *dynamodb.Key, attrs ...dynamodb.Attribute) *TError
	UpdateConditional(key *dynamodb.Key, attrs []dynamodb.Attribute, expected []dynamodb.Attribute) *TError
	Delete(key *dynamodb.Key) *TError
	DeleteConditional(key *dynamodb.Key, expected []dynamodb.Attribute) *TError
	ParallelScanPartialLimit([]dynamodb.AttributeComparison, *dynamodb.Key, int, int, int64) ([]map[string]*dynamodb.Attribute, *dynamodb.Key, *TError)
	Init() *TError
	Destroy() *TError
}

type TStore struct {
	dynamoServer *dynamodb.Server
	table        *dynamodb.Table
	tableDesc    *dynamodb.TableDescriptionT
	cfg          *TStoreConfig
}

type TStoreConfig struct {
	Auth                         aws.Auth
	Region                       aws.Region
	TableCreateCheckTimeout      string
	TableCreateCheckPollInterval string
}

func MakeDefaultStoreConfig() *TStoreConfig {
	return MakeStoreConfig(aws.Auth{}, DefaultRegion, DefaultTableCreateCheckTimeout, DefaultTableCreateCheckPollInterval)
}

func MakeStoreConfig(auth aws.Auth, region aws.Region, tableCreateTimeout, tableCreatePoll string) *TStoreConfig {
	return &TStoreConfig{auth, region, tableCreateTimeout, tableCreatePoll}
}

func MakeStore(tableDesc *dynamodb.TableDescriptionT, cfg *TStoreConfig) IStore {
	var (
		auth aws.Auth = cfg.Auth
		pk   dynamodb.PrimaryKey
	)
	contract.RequireNoErrors(
		func() (err error) {
			auth, err = aws.GetAuth(auth.AccessKey, auth.SecretKey, auth.Token(), auth.Expiration())
			return
		},
		func() (err error) {
			pk, err = tableDesc.BuildPrimaryKey()
			return
		})
	dynamo := dynamodb.Server{auth, cfg.Region}
	table := dynamo.NewTable(tableDesc.TableName, pk)
	repo := &TStore{&dynamo, table, tableDesc, cfg}
	return repo
}

func (self *TStore) findTableByName(name string) bool {
	log.WithField(LogTable, name).Debug("Searching for table in table list")
	tables, err := self.dynamoServer.ListTables()
	contract.RequireNoError(err)
	for _, t := range tables {
		if t == name {
			return true
		}
	}
	log.WithField(LogTable, name).Debug("Table not found")
	return false
}

func (self *TStore) Init() *TError {
	tableName := self.tableDesc.TableName
	log.WithField(LogTable, tableName).Debug("Initializing dnm.StoreStore")
	tableExists := self.findTableByName(tableName)
	if tableExists {
		log.WithField(LogTable, tableName).Debug("Waiting until table becomes active")
		self.waitUntilTableIsActive(tableName)
		return nil
	} else {
		log.WithField(LogTable, tableName).Info("Creating table")
		status, err := self.dynamoServer.CreateTable(*self.tableDesc)
		if err != nil {
			log.WithField(LogTable, tableName).Fatal("Unexpected error during dnm.StoreStore table intialization, cannot proceed")
			return self.makeError(InitGeneralErr, err)
		}
		if status == TableStatusCreating {
			log.WithField(LogTable, tableName).Debug("Waiting until table becomes active")
			self.waitUntilTableIsActive(tableName)
			return nil
		}
		if status == TableStatusActive {
			log.WithField(LogTable, tableName).Debug("Table is active")
			return nil
		}
		err = fmt.Errorf("Unexpected status: %s during dnm.StoreStore table intialization, cannot proceed", status)
		log.WithFields(log.Fields{
			fhlog.FHError: err,
			LogTable:      tableName,
		}).Fatal("Unexpected error during dnm.StoreStore table intialization, cannot proceed")
		return InitUnknownStatusErr
	}
}

func (self *TStore) waitUntilTableIsActive(table string) {
	checkTimeout, _ := time.ParseDuration(self.cfg.TableCreateCheckTimeout)
	checkInterval, _ := time.ParseDuration(self.cfg.TableCreateCheckPollInterval)
	ok, err := annoying.WaitUntil("table active", func() (status bool, err error) {
		status = false
		desc, err := self.dynamoServer.DescribeTable(table)
		if err != nil {
			return
		}
		if desc.TableStatus == TableStatusActive {
			status = true
			return
		}
		return
	}, checkInterval, checkTimeout)
	if !ok {
		log.WithFields(log.Fields{
			fhlog.FHError: err,
			LogTable:      table,
		}).Fatal("Failed waiting on table")
	}
}

func (self *TStore) Destroy() *TError {
	log.WithField(LogTable, self.tableDesc.TableName).Debug("Destroying table")
	tableExists := self.findTableByName(self.tableDesc.TableName)
	if !tableExists {
		log.WithField(LogTable, self.tableDesc.TableName).Debug("Table doesn't exists, skipping deletion")
		return nil
	} else {
		_, err := self.dynamoServer.DeleteTable(*self.tableDesc)
		if err != nil {
			log.WithFields(log.Fields{
				fhlog.FHError: err,
				LogTable:      self.tableDesc.TableName,
			}).Error("Error in Destroy()")

			return self.makeError(DestroyGeneralErr, err)
		}
		log.WithField(LogTable, self.tableDesc.TableName).Debug("Table deleted successfully")
	}
	return nil
}

func (self *TStore) Delete(key *dynamodb.Key) *TError {
	return self.DeleteConditional(key, nil)
}

func (self *TStore) DeleteConditional(key *dynamodb.Key, expected []dynamodb.Attribute) *TError {
	log.WithFields(log.Fields{
		LogKey:   key,
		LogTable: self.tableDesc.TableName,
	}).Debug("Deleting item with key")

	ok, err := self.table.ConditionalDeleteItem(key, expected)
	if ok {
		return nil
	} else {
		if strings.HasPrefix(err.Error(), ConditionalDynamoError) {
			return ConditionalErr
		}
		log.WithFields(log.Fields{
			LogKey:        key,
			LogTable:      self.tableDesc.TableName,
			fhlog.FHError: err.Error(),
		}).Error("Error in DeleteConditional()")
		return self.makeError(DeleteErr, err)
	}
}

func (self *TStore) Save(attrs ...dynamodb.Attribute) *TError {
	return self.SaveConditional(attrs, nil)
}

func (self *TStore) SaveConditional(attrs []dynamodb.Attribute, expected []dynamodb.Attribute) *TError {
	query := dynamodb.NewQuery(self.table)
	query.AddItem(attrs)
	if expected != nil {
		query.AddExpected(expected)
	}
	if _, err := self.table.RunPutItemQuery(query); err != nil {
		if strings.HasPrefix(err.Error(), ConditionalDynamoError) {
			return ConditionalErr
		} else {
			log.WithFields(log.Fields{
				LogQuery:      query.String(),
				LogTable:      self.tableDesc.TableName,
				fhlog.FHError: err.Error(),
			}).Error("Error in SaveConditional()")

			return self.makeError(SaveErr, err)
		}
	} else {
		return nil
	}
}

func (self *TStore) SaveConditionalWithConditionExpression(attrs []dynamodb.Attribute, condition *dynamodb.ConditionExpression) *TError {
	query := dynamodb.NewQuery(self.table)
	query.AddItem(attrs)
	if condition != nil {
		query.AddConditionExpression(condition)
	}
	if _, err := self.table.RunPutItemQuery(query); err != nil {
		if strings.HasPrefix(err.Error(), ConditionalDynamoError) {
			return ConditionalErr
		} else {
			log.WithFields(log.Fields{
				LogQuery:      query.String(),
				LogTable:      self.tableDesc.TableName,
				fhlog.FHError: err.Error(),
			}).Error("Error in SaveConditionalWithConditionExpression()")

			return self.makeError(SaveErr, err)
		}
	} else {
		return nil
	}
}

func (self *TStore) UpdateWithUpdateExpression(key *dynamodb.Key, returnValues string,
	attrs ...dynamodb.UpdateExpressionAttribute) (map[string]*dynamodb.Attribute, *TError) {
	if _, attrs, err := self.table.UpdateAttributesWithUpdateExpression(key, attrs, returnValues); err != nil {
		log.WithFields(log.Fields{
			LogKey:        key,
			LogAttributes: attrs,
			LogTable:      self.tableDesc.TableName,
			fhlog.FHError: err.Error(),
		}).Error("Error in UpdateWithUpdateExpression()")

		return nil, self.makeError(UpdateErr, err)
	} else {
		return attrs, nil
	}
}

func (self *TStore) UpdateConditionalWithUpdateExpression(key *dynamodb.Key, condition *dynamodb.ConditionExpression,
	returnValues string, attrs ...dynamodb.UpdateExpressionAttribute) (map[string]*dynamodb.Attribute, *TError) {

	if _, attrs, err := self.table.ConditionalUpdateAttributesWithUpdateExpression(key, attrs, condition, returnValues); err != nil {
		if strings.HasPrefix(err.Error(), ConditionalDynamoError) {
			return nil, ConditionalErr
		} else {
			log.WithFields(log.Fields{
				LogKey:          key,
				LogAttributes:   attrs,
				LogCondition:    condition,
				LogReturnValues: returnValues,
				LogTable:        self.tableDesc.TableName,
				fhlog.FHError:   err.Error(),
			}).Error("Error in UpdateConditionalWithUpdateExpression()")
			return nil, self.makeError(UpdateErr, err)
		}
	} else {
		return attrs, nil
	}
}

func (self *TStore) DeleteAttributesWithUpdateExpression(key *dynamodb.Key, returnValues string, attrs ...dynamodb.UpdateExpressionAttribute) (map[string]*dynamodb.Attribute, *TError) {

	if _, attrs, err := self.table.DeleteAttributesWithUpdateExpression(key, attrs, returnValues); err != nil {
		if strings.HasPrefix(err.Error(), ConditionalDynamoError) {
			return nil, ConditionalErr
		} else {
			log.WithFields(log.Fields{
				LogKey:          key,
				LogAttributes:   attrs,
				LogReturnValues: returnValues,
				LogTable:        self.tableDesc.TableName,
				fhlog.FHError:   err.Error(),
			}).Error("Error in DeleteAttributesWithUpdateExpression()")
			return nil, self.makeError(UpdateErr, err)
		}
	} else {
		return attrs, nil
	}
}

func (self *TStore) ModifyAttributesWithUpdateExpression(key *dynamodb.Key, condition *dynamodb.ConditionExpression,
	actions []string, returnValues string, attrs ...dynamodb.UpdateExpressionAttribute) (map[string]*dynamodb.Attribute, *TError) {

	if _, attrs, err := self.table.ModifyAttributesWithUpdateExpression(key, condition, attrs, actions, returnValues); err != nil {
		if strings.HasPrefix(err.Error(), ConditionalDynamoError) {
			return nil, ConditionalErr
		} else {
			log.WithFields(log.Fields{
				LogKey:          key,
				LogAttributes:   attrs,
				LogReturnValues: returnValues,
				LogTable:        self.tableDesc.TableName,
				fhlog.FHError:   err.Error(),
			}).Error("Error in ModifyAttributesWithUpdateExpression()")
			return nil, self.makeError(UpdateErr, err)
		}
	} else {
		return attrs, nil
	}
}

func (self *TStore) Update(key *dynamodb.Key, attrs ...dynamodb.Attribute) *TError {
	return self.UpdateConditional(key, attrs, nil)
}

func (self *TStore) UpdateConditional(key *dynamodb.Key, attrs []dynamodb.Attribute, expected []dynamodb.Attribute) *TError {
	if _, err := self.table.ConditionalUpdateAttributes(key, attrs, expected); err != nil {
		if strings.HasPrefix(err.Error(), ConditionalDynamoError) {
			return ConditionalErr
		} else {
			log.WithFields(log.Fields{
				LogKey:        key,
				LogAttributes: attrs,
				fhlog.FHError: err.Error(),
			}).Error("Error in UpdateConditional()")

			return self.makeError(UpdateErr, err)
		}
	} else {
		return nil
	}
}

func (self *TStore) Find(query *dynamodb.Query) ([]map[string]*dynamodb.Attribute, *TError) {
	if items, err := self.table.RunQuery(query); err != nil {
		log.WithFields(log.Fields{
			LogQuery:      query.String(),
			LogTable:      self.tableDesc.TableName,
			fhlog.FHError: err.Error(),
		}).Error("Error in Find()")

		return nil, self.makeError(LookupErr, err)
	} else {
		return items, nil
	}
}

func (self *TStore) Get(key *dynamodb.Key) (map[string]*dynamodb.Attribute, *TError) {
	if attrMap, err := self.table.GetItem(key); err != nil {
		if err == dynamodb.ErrNotFound {
			return nil, NotFoundErr
		} else {
			log.WithFields(log.Fields{
				LogKey:        key,
				LogTable:      self.tableDesc.TableName,
				fhlog.FHError: err.Error(),
			}).Error("Error in Get()")

			return nil, self.makeError(LookupErr, err)
		}
	} else {
		return attrMap, nil
	}
}

func (self *TStore) ParallelScanPartialLimit(attributeComparisons []dynamodb.AttributeComparison, exclusiveStartKey *dynamodb.Key,
	segment, totalSegments int, limit int64) ([]map[string]*dynamodb.Attribute, *dynamodb.Key, *TError) {

	if attrMap, key, err := self.table.ParallelScanPartialLimit(attributeComparisons, exclusiveStartKey,
		segment, totalSegments, limit); err != nil {

		if err == dynamodb.ErrNotFound {
			return nil, nil, NotFoundErr
		} else {
			log.WithFields(log.Fields{
				LogKey:                  key,
				LogTable:                self.tableDesc.TableName,
				LogAttributeComparisons: attributeComparisons,
				LogExclusiveStartKey:    exclusiveStartKey,
				LogSegment:              segment,
				LogTotalSegments:        totalSegments,
				LogLimit:                limit,
				fhlog.FHError:           err.Error(),
			}).Error("Error in ParallelScanPartialLimit()")

			return nil, nil, self.makeError(LookupErr, err)
		}
	} else {
		return attrMap, key, nil
	}
}

func (self *TStore) makeError(tErr *TError, details error) *TError {
	return MakeError(tErr.Summary, fmt.Sprintf("table: %s, err: %v, desc: %s", self.table.Name, details, tErr.Description))
}
