package dnm

import (
	"fmt"
	"github.com/flowhealth/glog"
	"github.com/flowhealth/goamz/aws"
	"github.com/flowhealth/goamz/dynamodb"
	"github.com/flowhealth/goannoying"
	"github.com/flowhealth/gocontract/contract"
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
	UpdateAttributesWithUpdateExpression(key *dynamodb.Key, attrs ...dynamodb.UpdateExpressionAttribute) *TError
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
	glog.V(5).Infof("Searching for table %s in table list", name)
	tables, err := self.dynamoServer.ListTables()
	glog.V(5).Infof("Got table list: %v", tables)
	contract.RequireNoError(err)
	for _, t := range tables {
		if t == name {
			glog.V(5).Infof("Found table %s", name)
			return true
		}
	}
	glog.V(5).Infof("Table %s wasnt found", name)
	return false
}

func (self *TStore) Init() *TError {
	tableName := self.tableDesc.TableName
	glog.V(3).Infof("Initializing dnm.StoreStore(%s) table", tableName)
	tableExists := self.findTableByName(tableName)
	if tableExists {
		glog.V(3).Infof("dnm.StoreStore table '%s' exists, skipping init", tableName)
		glog.V(3).Infof("Waiting until table '%s' becomes active", tableName)
		self.waitUntilTableIsActive(tableName)
		glog.V(3).Infof("dnm.StoreStore table '%s' is active", tableName)
		return nil
	} else {
		glog.Infof("Creating dnm.StoreStore table '%s'", tableName)
		status, err := self.dynamoServer.CreateTable(*self.tableDesc)
		if err != nil {
			glog.Fatalf("Unexpected error: %s during dnm.StoreStore table intialization, cannot proceed", err.Error())
			return self.makeError(InitGeneralErr, err)
		}
		if status == TableStatusCreating {
			glog.V(3).Infof("Waiting until dnm.StoreStore table '%s' becomes active", tableName)
			self.waitUntilTableIsActive(tableName)
			glog.V(3).Infof("dnm.StoreStore table '%s' become active", tableName)
			return nil
		}
		if status == TableStatusActive {
			glog.V(3).Infof("dnm.StoreStore table '%s' is active", tableName)
			return nil
		}
		err = fmt.Errorf("Unexpected status: %s during dnm.StoreStore table intialization, cannot proceed", status)
		glog.Fatal(err)
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
		glog.Fatalf("Failed with: %s", err.Error())
	}
}

func (self *TStore) Destroy() *TError {
	glog.Info("Destroying tables")
	tableExists := self.findTableByName(self.tableDesc.TableName)
	if !tableExists {
		glog.Infof("Table %s doesn't exists, skipping deletion", self.tableDesc.TableName)
		return nil
	} else {
		_, err := self.dynamoServer.DeleteTable(*self.tableDesc)
		if err != nil {
			glog.Fatal(err)
			return self.makeError(DestroyGeneralErr, err)
		}
		glog.Infof("Table %s deleted successfully", self.tableDesc.TableName)
	}
	return nil
}

func (self *TStore) Delete(key *dynamodb.Key) *TError {
	return self.DeleteConditional(key, nil)
}

func (self *TStore) DeleteConditional(key *dynamodb.Key, expected []dynamodb.Attribute) *TError {
	glog.V(5).Infof("Deleting item with key : %s", key)
	ok, err := self.table.ConditionalDeleteItem(key, expected)
	if ok {
		glog.V(5).Infof("Succeed delete item : %s", key)
		return nil
	} else {
		if strings.HasPrefix(err.Error(), ConditionalDynamoError) {
			return ConditionalErr
		} else {
			glog.Errorf("Failed to delete item: %s, error: %v", key, err)
		}
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
			glog.Errorf("Failed save query: %s, error: %v", query.String(), err)
			return self.makeError(SaveErr, err)
		}
	} else {
		return nil
	}
}

func (self *TStore) UpdateAttributesWithUpdateExpression(key *dynamodb.Key, attrs ...dynamodb.UpdateExpressionAttribute) *TError {
	if _, err := self.table.UpdateAttributesWithUpdateExpression(key, attrs); err != nil {
		glog.Errorf("Failed update item: %v, with attributes %v, error: %v", key, attrs, err)
		return self.makeError(UpdateErr, err)
	} else {
		return nil
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
			glog.Errorf("Failed update item: %v, with attributes %v, error: %v", key, attrs, err)
			return self.makeError(UpdateErr, err)
		}
	} else {
		return nil
	}
}

func (self *TStore) Find(query *dynamodb.Query) ([]map[string]*dynamodb.Attribute, *TError) {
	if items, err := self.table.RunQuery(query); err != nil {
		glog.Errorf("Failed query: %s, error: %v", query.String(), err)
		return nil, self.makeError(LookupErr, err)
	} else {
		glog.V(5).Infof("Succeed item %#v fetch, got: %v", query, items)
		return items, nil
	}
}

func (self *TStore) Get(key *dynamodb.Key) (map[string]*dynamodb.Attribute, *TError) {
	glog.V(5).Infof("Getting item with pk: %s", key)
	if attrMap, err := self.table.GetItem(key); err != nil {
		if err == dynamodb.ErrNotFound {
			return nil, NotFoundErr
		} else {
			glog.Errorf("Failed to lookup an item with key %#v, because: %v", key, err)
			return nil, self.makeError(LookupErr, err)
		}
	} else {
		glog.V(5).Infof("Succeed item %s fetch, got: %v", key, attrMap)
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
			glog.Errorf("Failed to scan, attributeComparisons: %v, exclusiveStartKey: %v, segment: %d, totalSegments: %d, limit: %d, err: %v",
				attributeComparisons, exclusiveStartKey, segment, totalSegments, limit, err)
			return nil, nil, self.makeError(LookupErr, err)
		}
	} else {
		glog.V(5).Infof("Succeed item %s fetch, got: %v", key, attrMap)
		return attrMap, key, nil
	}
}

func (self *TStore) makeError(tErr *TError, details error) *TError {
	return MakeError(tErr.Summary, fmt.Sprintf("table: %s, err: %v, desc: %s", self.table.Name, details, tErr.Description))
}
