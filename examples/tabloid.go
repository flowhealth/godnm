package main

import (
	"fmt"
	"github.com/superduper/godnm/dnm"
)

func main() {
	d := dnm.Describe("Threads", func(t dnm.ITable) {
		//
		// Attribute definitions
		//
		forumName := t.Attr("ForumName", dnm.String)
		_ = t.Attr("LastPostDateTime", dnm.String)
		subject := t.Attr("Subject", dnm.String)
		//lastPostDateTime := t.Attr("LastPostDateTime", dnm.String)
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
		//
		// Local Indexes
		//
		{
			g := t.LocalIndex("OtherIndex")
			g.Range(forumName)
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
	fmt.Printf("%+v", d)
}
