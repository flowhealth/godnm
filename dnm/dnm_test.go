package dnm_test

import (
	"github.com/crowdmob/goamz/dynamodb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/superduper/godnm/dnm"
)

func assertHasKey(d []dynamodb.KeySchemaT, name, typ string) bool {
	for _, v := range d {
		if v.AttributeName == name || v.KeyType == typ {
			return true
		}
	}
	return false
}

func assertHasNonKeyAttr(d dynamodb.ProjectionT, name string) bool {
	for _, v := range d.NonKeyAttributes {
		if v == name {
			return true
		}
	}
	return false
}

func assertHasAttr(d dynamodb.TableDescriptionT, name, typ string) bool {
	for _, v := range d.AttributeDefinitions {
		if v.Name == name || v.Type == typ {
			return true
		}
	}
	return false
}

var _ = Describe("Dnm", func() {
	d := dnm.Describe("Threads", func(t dnm.ITable) {
		//
		// Attribute definitions
		//
		forumName := t.Attr("ForumName", dnm.String)
		subject := t.Attr("Subject", dnm.String)
		created := t.Attr("Created", dnm.Number)
		userId := t.Attr("UserId", dnm.String)
		//
		// Primary Key
		//
		{
			pk := t.PrimaryKey()
			pk.Hash(forumName)
			pk.Range(created)
		}
		//
		// Provisioning
		//
		{
			p := t.ProvisionedThroughput()
			p.WriteCapacity(1)
			p.ReadCapacity(1)
		}
		//)
		// Local Indexes
		//
		{
			g := t.LocalIndex("OtherIndex")
			g.Range(subject)
			g.Projection().All()
		}
		//
		// Global Indexes
		//
		{
			g := t.GlobalIndex("UserIndex")
			g.Hash(userId)
			g.Range(forumName)
			g.Projection().Include(subject)
			{
				p := g.ProvisionedThroughput()
				p.WriteCapacity(1)
				p.ReadCapacity(1)
			}
		}
	})

	li := d.LocalSecondaryIndexes[0]
	gi := d.GlobalSecondaryIndexes[0]

	Context("Basic definition", func() {
		It("should have correct table name", func() {
			Expect(d.TableName).To(Equal("Threads"))
		})
		It("should have declared attribute definitions", func() {
			Expect(assertHasAttr(d, "ForumName", dnm.String)).To(BeTrue())
			Expect(assertHasAttr(d, "Subject", dnm.String)).To(BeTrue())
			Expect(assertHasAttr(d, "Created", dnm.Number)).To(BeTrue())
			Expect(assertHasAttr(d, "UserId", dnm.String)).To(BeTrue())
		})
		It("should have correct primary key schema", func() {
			Expect(assertHasKey(d.KeySchema, "ForumName", dnm.KeyHash)).To(BeTrue())
			Expect(assertHasKey(d.KeySchema, "Created", dnm.KeyRange)).To(BeTrue())
		})
		It("should have correct provisioned throughput", func() {
			Expect(d.ProvisionedThroughput.ReadCapacityUnits).To(Equal(int64(1)))
			Expect(d.ProvisionedThroughput.WriteCapacityUnits).To(Equal(int64(1)))
		})
	})
	Context("Local secondary index", func() {
		It("should have index", func() {
			Expect(li).ToNot(BeNil())
		})
		It("should have correct index name", func() {
			Expect(li.IndexName).To(Equal("OtherIndex"))
		})
		It("should have correct key schema", func() {
			Expect(assertHasKey(li.KeySchema, "Subject", dnm.KeyRange)).To(BeTrue())
		})
		It("should have correct projection settings", func() {
			Expect(li.Projection.ProjectionType).To(Equal(dnm.ProjectionTypeAll))
			Expect(li.Projection.NonKeyAttributes).To(HaveLen(0))
		})
	})
	Context("Global secondary index", func() {
		It("should have index", func() {
			Expect(gi).ToNot(BeNil())
		})
		It("should have correct key schema", func() {
			Expect(assertHasKey(gi.KeySchema, "UserId", dnm.KeyHash)).To(BeTrue())
			Expect(assertHasKey(gi.KeySchema, "ForumName", dnm.KeyRange)).To(BeTrue())
		})
		It("should have correct provisioned throughput", func() {
			Expect(gi.ProvisionedThroughput.ReadCapacityUnits).To(Equal(int64(1)))
			Expect(gi.ProvisionedThroughput.WriteCapacityUnits).To(Equal(int64(1)))
		})
		It("should have correct projection settings", func() {
			Expect(gi.Projection.ProjectionType).To(Equal(dnm.ProjectionTypeInclude))
			Expect(assertHasNonKeyAttr(gi.Projection, "Subject")).To(BeTrue())
			Expect(gi.Projection.NonKeyAttributes).To(HaveLen(1))
		})
	})
})
