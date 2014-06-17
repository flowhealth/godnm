package dnm

import "github.com/crowdmob/goamz/dynamodb"

type tProvisionedThroughput struct {
	pt *dynamodb.ProvisionedThroughputT
}

type iProvisionedThroughput interface {
	ReadCapacity(int64)
	WriteCapacity(int64)
}

func makeProvisionedThroughput(pt *dynamodb.ProvisionedThroughputT) iProvisionedThroughput {
	return &tProvisionedThroughput{pt}
}

func (self *tProvisionedThroughput) WriteCapacity(n int64) {
	if n < 1 {
		panic("Incorrect table definition: provisioned throughput write capacity cant be less 1")
	}
	self.pt.WriteCapacityUnits = n

}

func (self *tProvisionedThroughput) ReadCapacity(n int64) {
	if n < 1 {
		panic("Incorrect table definition: provisioned throughput read capacity cant be less 1")
	}
	self.pt.ReadCapacityUnits = n
}
