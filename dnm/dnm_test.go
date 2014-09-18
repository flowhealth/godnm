package dnm_test

import (
	"math"
	"time"

	"github.com/flowhealth/goamz/dynamodb"
	"github.com/flowhealth/godnm/dnm"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

func toItemAttrs(attrs ...dynamodb.Attribute) map[string]*dynamodb.Attribute {
	m := map[string]*dynamodb.Attribute{}
	for _, v := range attrs {
		// stupid way to copy
		tmp := &v
		tmp1 := *tmp
		m[v.Name] = &tmp1
	}
	return m
}

var _ = Describe("Dnm", func() {
	d := dnm.Describe("Threads", func(t dnm.ITable) {
		//
		// Attribute definitions
		//
		forumName := t.KeyAttr("ForumName", dnm.String)
		subject := t.KeyAttr("Subject", dnm.String)
		created := t.KeyAttr("Created", dnm.Number)
		userId := t.KeyAttr("UserId", dnm.String)
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

	Context("Definition helpers", func() {
		Context("Table modifications", func() {
			d := dnm.Describe("Simple table", func(t dnm.ITable) {
				//
				// Attribute definitions
				//
				forumName := t.KeyAttr("ForumName").AsString()
				created := t.KeyAttr("Created").AsTimeTime()
				blob := t.KeyAttr("Blob").AsBinary()
				enabled := t.KeyAttr("Enabled").AsBool()

				count := t.KeyAttr("Count").AsInt()
				count32 := t.KeyAttr("Count32").AsInt32()
				count64 := t.KeyAttr("Count64").AsInt64()

				weight32 := t.KeyAttr("Weight32").AsFloat32()
				weight64 := t.KeyAttr("Weight64").AsFloat64()

				// String

				It("describe string attribute", func() {
					Expect(forumName.Def().Type).To(Equal(dnm.String))
					Expect(forumName.Def().Name).To(Equal("ForumName"))
				})

				// Binary

				It("describe binary attribute", func() {
					Expect(blob.Def().Type).To(Equal(dnm.Binary))
					Expect(blob.Def().Name).To(Equal("Blob"))
				})

				// Time.Time

				It("describe time.Time attribute", func() {
					Expect(created.Def().Type).To(Equal(dnm.Number))
					Expect(created.Def().Name).To(Equal("Created"))
				})

				// Boolean

				It("describe bool attribute", func() {
					Expect(enabled.Def().Type).To(Equal(dnm.Number))
					Expect(enabled.Def().Name).To(Equal("Enabled"))
				})

				// int

				It("describe int attribute", func() {
					Expect(count.Def().Type).To(Equal(dnm.Number))
					Expect(count.Def().Name).To(Equal("Count"))
				})

				// int32

				It("describe int32 attribute", func() {
					Expect(count32.Def().Type).To(Equal(dnm.Number))
					Expect(count32.Def().Name).To(Equal("Count32"))
				})

				// int64

				It("describe int64 attribute", func() {
					Expect(count64.Def().Type).To(Equal(dnm.Number))
					Expect(count64.Def().Name).To(Equal("Count64"))
				})

				// float32

				It("describe float32 attribute", func() {
					Expect(weight32.Def().Type).To(Equal(dnm.Number))
					Expect(weight32.Def().Name).To(Equal("Weight32"))
				})

				// float64

				It("describe float64 attribute", func() {
					Expect(weight64.Def().Type).To(Equal(dnm.Number))
					Expect(weight64.Def().Name).To(Equal("Weight64"))
				})

			})

			// String

			It("describe string attribute", func() {
				Expect(assertHasAttr(d, "ForumName", dnm.String)).To(BeTrue())
			})

			// Binary

			It("describe binary attribute", func() {
				Expect(assertHasAttr(d, "Blob", dnm.Binary)).To(BeTrue())
			})

			// Time.Time

			It("describe time.Time attribute", func() {
				Expect(assertHasAttr(d, "Created", dnm.Number)).To(BeTrue())
			})

			// Boolean

			It("describe bool attribute", func() {
				Expect(assertHasAttr(d, "Enabled", dnm.Number)).To(BeTrue())
			})

			// int

			It("describe int attribute", func() {
				Expect(assertHasAttr(d, "Count", dnm.Number)).To(BeTrue())
			})

			// int32

			It("describe int32 attribute", func() {
				Expect(assertHasAttr(d, "Count32", dnm.Number)).To(BeTrue())
			})

			// int64

			It("describe int64 attribute", func() {
				Expect(assertHasAttr(d, "Count64", dnm.Number)).To(BeTrue())
			})

			// float32

			It("describe float32 attribute", func() {
				Expect(assertHasAttr(d, "Weight32", dnm.Number)).To(BeTrue())
			})

			// float64

			It("describe float64 attribute", func() {
				Expect(assertHasAttr(d, "Weight64", dnm.Number)).To(BeTrue())
			})
		})

	})

	Context("Serialization helpers", func() {
		_ = dnm.Describe("Simple table", func(t dnm.ITable) {
			//
			// Attribute definitions
			//
			forumName := t.KeyAttr("ForumName").AsString()
			created := t.KeyAttr("Created").AsTimeTime()
			blob := t.KeyAttr("Blob").AsBinary()
			enabled := t.KeyAttr("Enabled").AsBool()
			banned := t.KeyAttr("Banned").AsBool()

			count := t.KeyAttr("Count").AsInt()
			count32 := t.KeyAttr("Count32").AsInt32()
			count64 := t.KeyAttr("Count64").AsInt64()

			weight32 := t.KeyAttr("Weight32").AsFloat32()
			weight64 := t.KeyAttr("Weight64").AsFloat64()

			strVal := "Boo"
			timeVal := time.Now()
			boolValF := false
			boolValT := true

			intVal := int(math.MaxInt64)
			intVal32 := int32(math.MaxInt32)
			intVal64 := int64(math.MaxInt64)

			floatVal32 := float32(math.MaxFloat32)
			floatVal64 := float64(math.SmallestNonzeroFloat64)
			binVal := []byte(`прекрасное далёко, не будь ко мне жестоко...`)

			attrs := toItemAttrs(forumName.Is(strVal),
				created.Is(timeVal),
				blob.Is(binVal),
				enabled.Is(boolValF),
				banned.Is(boolValT),
				count.Is(intVal),
				count32.Is(intVal32),
				count64.Is(intVal64),
				weight32.Is(floatVal32),
				weight64.Is(floatVal64),
			)

			// String

			It("describe string attribute", func() {
				Expect(forumName.Def().Type).To(Equal(dnm.String))
				Expect(forumName.Def().Name).To(Equal("ForumName"))
			})

			It("should serialize string", func() {
				Expect(forumName.Is(strVal).Value).To(Equal(strVal))
				Expect(forumName.Is(strVal).Type).To(Equal(dnm.String))
			})

			It("should de-serialize string", func() {
				v, err := forumName.From(attrs)
				Expect(strVal).To(Equal(v))
				Expect(err).To(BeNil())
			})

			// Binary

			It("describe binary attribute", func() {
				Expect(blob.Def().Type).To(Equal(dnm.Binary))
				Expect(blob.Def().Name).To(Equal("Blob"))
			})

			It("should serialize binary", func() {
				v := dnm.FromBinary(binVal)
				Expect(blob.Is(binVal).Value).To(Equal(v))
				Expect(blob.Is(binVal).Type).To(Equal(dnm.Binary))
			})

			It("should de-serialize binary", func() {
				v, err := blob.From(attrs)
				Expect(err).To(BeNil())
				Expect(binVal).To(Equal(v))
			})

			// Time.Time

			It("describe time.Time attribute", func() {
				Expect(created.Def().Type).To(Equal(dnm.Number))
				Expect(created.Def().Name).To(Equal("Created"))
			})

			It("should serialize time.Time", func() {
				v := dnm.FromTimeTime(timeVal)
				Expect(created.Is(timeVal).Value).To(Equal(v))
				Expect(created.Is(timeVal).Type).To(Equal(dnm.Number))
			})

			It("should de-serialize time.Time", func() {
				v, err := created.From(attrs)
				Expect(timeVal.Unix()).To(Equal(v.Unix()))
				Expect(err).To(BeNil())
			})

			// Boolean

			It("describe bool attribute", func() {
				Expect(enabled.Def().Type).To(Equal(dnm.Number))
				Expect(enabled.Def().Name).To(Equal("Enabled"))
			})
			// when false
			It("should serialize false bool", func() {
				Expect(enabled.Is(boolValF).Value).To(Equal(string(dnm.DynamoBoolFalse)))
				Expect(enabled.Is(boolValF).Type).To(Equal(dnm.Number))
			})

			It("should de-serialize false bool", func() {
				v, err := enabled.From(attrs)
				Expect(boolValF).To(Equal(v))
				Expect(err).To(BeNil())
			})

			// when true
			It("should serialize true bool", func() {
				Expect(banned.Is(boolValT).Value).To(Equal(string(dnm.DynamoBoolTrue)))
				Expect(banned.Is(boolValT).Type).To(Equal(dnm.Number))
			})

			It("should de-serialize true bool", func() {
				v, err := banned.From(attrs)
				Expect(boolValT).To(Equal(v))
				Expect(err).To(BeNil())
			})

			// int

			It("describe int attribute", func() {
				Expect(count.Def().Type).To(Equal(dnm.Number))
				Expect(count.Def().Name).To(Equal("Count"))
			})

			It("should serialize int", func() {
				v := dnm.FromInt(intVal)
				Expect(count.Is(intVal).Value).To(Equal(v))
				Expect(count.Is(intVal).Type).To(Equal(dnm.Number))
			})

			It("should de-serialize int", func() {
				v, err := count.From(attrs)
				Expect(err).To(BeNil())
				Expect(intVal).To(Equal(v))
			})

			// int32

			It("describe int32 attribute", func() {
				Expect(count32.Def().Type).To(Equal(dnm.Number))
				Expect(count32.Def().Name).To(Equal("Count32"))
			})

			It("should serialize int32", func() {
				v := dnm.FromInt32(intVal32)
				Expect(count32.Is(intVal32).Value).To(Equal(v))
				Expect(count32.Is(intVal32).Type).To(Equal(dnm.Number))
			})

			It("should de-serialize int32", func() {
				v, err := count32.From(attrs)
				Expect(intVal32).To(Equal(v))
				Expect(err).To(BeNil())
			})

			// int64

			It("describe int64 attribute", func() {
				Expect(count64.Def().Type).To(Equal(dnm.Number))
				Expect(count64.Def().Name).To(Equal("Count64"))
			})

			It("should serialize int64", func() {
				v := dnm.FromInt64(intVal64)
				Expect(count64.Is(intVal64).Value).To(Equal(v))
				Expect(count64.Is(intVal64).Type).To(Equal(dnm.Number))
			})

			It("should de-serialize int64", func() {
				v, err := count64.From(attrs)
				Expect(intVal64).To(Equal(v))
				Expect(err).To(BeNil())
			})

			// float32

			It("describe float32 attribute", func() {
				Expect(weight32.Def().Type).To(Equal(dnm.Number))
				Expect(weight32.Def().Name).To(Equal("Weight32"))
			})

			It("should serialize float32", func() {
				v := dnm.FromFloat32(floatVal32)
				Expect(weight32.Is(floatVal32).Value).To(Equal(v))
				Expect(weight32.Is(floatVal32).Type).To(Equal(dnm.Number))
			})

			It("should de-serialize float32", func() {
				v, err := weight32.From(attrs)
				Expect(floatVal32).To(Equal(v))
				Expect(err).To(BeNil())
			})

			// float64

			It("describe float64 attribute", func() {
				Expect(weight64.Def().Type).To(Equal(dnm.Number))
				Expect(weight64.Def().Name).To(Equal("Weight64"))
			})

			It("should serialize float64", func() {
				v := dnm.FromFloat64(floatVal64)
				Expect(weight64.Is(floatVal64).Value).To(Equal(v))
				Expect(weight64.Is(floatVal64).Type).To(Equal(dnm.Number))
			})

			It("should de-serialize float64", func() {
				v, err := weight64.From(attrs)
				Expect(floatVal64).To(Equal(v))
				Expect(err).To(BeNil())
			})
		})
	})
})
