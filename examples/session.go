package main

import (
	"fmt"
	"github.com/flowhealth/godnm/dnm"
)

func main() {
	// define our table
	var (
		Id, UserId, IpAddr, UserAgent dnm.IAttr
		UserIndex                     dnm.IIndex
	)
	d := dnm.Describe("Sessions-Test", func(t dnm.ITable) {
		//
		// Key Attribute definitions
		//
		Id = t.KeyAttr("Id", dnm.String)
		UserId = t.KeyAttr("UserId", dnm.String)
		IpAddr = t.KeyAttr("IpAddr", dnm.String)
		UserAgent = t.NonKeyAttr("UserAgent", dnm.String)
		{
			t.PrimaryKey().Hash(Id)
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
		// Global Indexes
		//
		{
			idx := t.GlobalIndex("UserIndex")
			idx.Hash(UserId)
			idx.Projection().All()
			{
				p := idx.ProvisionedThroughput()
				p.WriteCapacity(1)
				p.ReadCapacity(1)
			}
			UserIndex = idx
		}
		//
		// Global Indexes
		//
		{
			idx := t.GlobalIndex("IpAddrIndex")
			idx.Hash(IpAddr)
			idx.Range(Id)
			idx.Projection().All()
			{
				p := idx.ProvisionedThroughput()
				p.WriteCapacity(1)
				p.ReadCapacity(1)
			}
		}
	})
	// init store
	cfg := dnm.MakeDefaultStoreConfig()
	store := dnm.MakeStore(&d, cfg)
	store.Init()
	// insert several records
	{
		sid := "sid:1"
		userId := "uid:1"
		userAgent := "ua:ie"
		ipAddr := "127.0.0.1"

		store.Save(sid,
			UserId.Is(userId),
			UserAgent.Is(userAgent),
			IpAddr.Is(ipAddr),
		)
	}
	{
		sid := "sid:2"
		userId := "uid:1"
		userAgent := "ua:ie"
		ipAddr := "127.0.0.1"

		store.Save(sid,
			UserId.Is(userId),
			UserAgent.Is(userAgent),
			IpAddr.Is(ipAddr),
		)
	}
	// query against global secondary index
	{
		userId := "uid:1"
		q := UserIndex.Where(UserId.Equals(userId))
		if items, err := store.Find(q); err != nil {
			panic(fmt.Sprint(err.Error(), err.Description))
		} else {
			fmt.Printf("%#v", items)
		}
	}
	// try to get model
	{
		sid := "sid:1"
		if attrs, err := store.Get(sid); err != nil {
			panic(err.Error())
		} else {
			fmt.Println("got user id", UserId.From(attrs))
			fmt.Println("got user agent", UserAgent.From(attrs))
			fmt.Println("got ip addr", IpAddr.From(attrs))
		}
	}
	// try to delete model
	{
		sid := "sid:1"
		if err := store.Delete(sid); err != nil {
			panic(err.Error())
		}
	}
}
