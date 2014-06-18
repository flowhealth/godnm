package dnm

import (
	"fmt"
	"github.com/flowhealth/glog"
	"github.com/flowhealth/goamz/aws"
	"github.com/flowhealth/goamz/dynamodb"
	"github.com/flowhealth/goannoying"
	"github.com/flowhealth/gocontract/contract"
	"time"
)

const (
	PrimaryKeyName                      = "Key"
	TableStatusActive                   = "ACTIVE"
	TableStatusCreating                 = "CREATING"
	DefaultTableCreateCheckTimeout      = "20s"
	DefaultTableCreateCheckPollInterval = "3s"
	DefaultReadCapacity                 = 1
	DefaultWriteCapacity                = 1
)

var (
	DefaultRegion = aws.USWest2
)

type IStore interface {
	Get(string) (map[string]*dynamodb.Attribute, *TError)
	Query(query *dynamodb.Query) ([]map[string]*dynamodb.Attribute, *TError)
	Save(string, ...dynamodb.Attribute) *TError
	Delete(string) *TError
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
	Region                       aws.Region
	TableCreateCheckTimeout      string
	TableCreateCheckPollInterval string
}

func MakeDefaultStoreConfig() *TStoreConfig {
	return MakeStoreConfig(DefaultRegion, DefaultTableCreateCheckTimeout, DefaultTableCreateCheckPollInterval)
}

func MakeStoreConfig(region aws.Region, tableCreateTimeout, tableCreatePoll string) *TStoreConfig {
	return &TStoreConfig{region, tableCreateTimeout, tableCreatePoll}
}

func MakeStore(tableDesc *dynamodb.TableDescriptionT, cfg *TStoreConfig) IStore {
	var (
		auth aws.Auth
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
	repo.Init()
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
	glog.V(3).Infof("Initializing KeyAttrStore(%s) table", tableName)
	tableExists := self.findTableByName(tableName)
	if tableExists {
		glog.V(3).Infof("KeyAttrStore table '%s' exists, skipping init", tableName)
		glog.V(3).Infof("Waiting until table '%s' becomes active", tableName)
		self.waitUntilTableIsActive(tableName)
		glog.V(3).Infof("KeyAttrStore table '%s' is active", tableName)
		return nil
	} else {
		glog.Infof("Creating KeyAttrStore table '%s'", tableName)
		status, err := self.dynamoServer.CreateTable(*self.tableDesc)
		if err != nil {
			glog.Fatalf("Unexpected error: %s during KeyAttrStore table intialization, cannot proceed", err.Error())
			return InitGeneralErr
		}
		if status == TableStatusCreating {
			glog.V(3).Infof("Waiting until KeyAttrStore table '%s' becomes active", tableName)
			self.waitUntilTableIsActive(tableName)
			glog.V(3).Infof("KeyAttrStore table '%s' become active", tableName)
			return nil
		}
		if status == TableStatusActive {
			glog.V(3).Infof("KeyAttrStore table '%s' is active", tableName)
			return nil
		}
		err = fmt.Errorf("Unexpected status: %s during KeyAttrStore table intialization, cannot proceed", status)
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
			return DestroyGeneralErr
		}
		glog.Infof("Table %s deleted successfully", self.tableDesc.TableName)
	}
	return nil
}

func (self *TStore) Delete(key string) *TError {
	glog.V(5).Infof("Deleting item with key : %s", key)
	ok, err := self.table.DeleteItem(makeItemKey(key))
	if ok {
		glog.V(5).Infof("Succeed delete item : %s", key)
		return nil
	} else {
		glog.Errorf("Failed to delete item : %s, because of:%s", key, err.Error())
		return DeleteErr
	}
}

func (self *TStore) Save(key string, attrs ...dynamodb.Attribute) *TError {
	glog.V(5).Infof("Saving item with key : %s", key)
	if ok, err := self.table.PutItem(key, "", attrs); ok {
		return nil
	} else {
		glog.Errorf("Failed to save because : %s", err.Error())
		return SaveErr
	}
}

func (self *TStore) QueryConsistent(query *dynamodb.Query) ([]map[string]*dynamodb.Attribute, *TError) {
	query.ConsistentRead(true)
	return self.Query(query)
}

func (self *TStore) Query(query *dynamodb.Query) ([]map[string]*dynamodb.Attribute, *TError) {
	if items, err := self.table.RunQuery(query); err != nil {
		glog.Errorf("Failed query: %s", query.String())
		return nil, MakeError(LookupErr.Summary, err.Error())
	} else {
		glog.V(5).Infof("Succeed item %#v fetch, got: %v", query, items)
		return items, nil
	}
}

func (self *TStore) Get(key string) (map[string]*dynamodb.Attribute, *TError) {
	glog.V(5).Infof("Getting item with pk: %s", key)
	attrMap, _err := self.table.GetItem(makeItemKey(key))
	if _err == nil {
		glog.V(5).Infof("Succeed item %s fetch, got: %v", key, attrMap)
	} else {
		if _err == dynamodb.ErrNotFound {
			return nil, NotFoundErr
		} else {
			return nil, LookupErr
		}
	}
	return attrMap, nil
}

func makeItemKey(key string) *dynamodb.Key {
	return &dynamodb.Key{HashKey: key}
}
