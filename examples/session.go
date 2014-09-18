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
		PKIndex                       dnm.IKeyFactory
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
			pk := t.PrimaryKey()
			pk.Hash(Id)
			PKIndex = pk.Factory()
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
			idx.Range(Id)
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

		if err := store.Save(Id.Is(sid),
			UserId.Is(userId),
			UserAgent.Is(userAgent),
			IpAddr.Is(ipAddr),
		); err != nil {
			panic(fmt.Sprint("Failed to insert", sid, "because of", err.Error()))
		} else {
			fmt.Println("Insert ok for", sid)
		}
	}
	{
		sid := "sid:2"
		userId := "uid:1"
		userAgent := "ua:ie"
		ipAddr := "127.0.0.1"
		if err := store.Save(
			Id.Is(sid),
			UserId.Is(userId),
			UserAgent.Is(userAgent),
			IpAddr.Is(ipAddr),
		); err != nil {
			panic(fmt.Sprint("Failed to insert", sid, "because of", err.Error()))
		} else {
			fmt.Println("Insert ok for", sid)
		}

	}
	// query against global secondary index
	{
		userId := "uid:1"
		q := UserIndex.Where(UserId.Equals(userId))
		if items, err := store.Find(q); err != nil {
			panic(fmt.Sprint(err.Error(), err.Description))
		} else {
			for n, attrs := range items {
				fmt.Printf("sess #%d\n", n)
				fmt.Println("got user id", UserId.From(attrs))
				fmt.Println("got user agent", UserAgent.From(attrs))
				fmt.Println("got ip addr", IpAddr.From(attrs))
			}
		}
	}
	// query against global secondary index
	{
		userId := "uid:1"
		q := UserIndex.Where(UserId.Equals(userId))
		if items, err := store.Find(q); err != nil {
			panic(fmt.Sprint(err.Error(), err.Description))
		} else {
			for n, attrs := range items {
				fmt.Printf("sess #%d\n", n)
				fmt.Println("got user id", UserId.From(attrs))
				fmt.Println("got user agent", UserAgent.From(attrs))
				fmt.Println("got ip addr", IpAddr.From(attrs))
			}
		}
	}
	// try to get model
	{
		sid := "sid:1"
		pk := PKIndex.Key(Id.Is(sid))
		if attrs, err := store.Get(&pk); err != nil {
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
		pk := PKIndex.Key(Id.Is(sid))
		if err := store.Delete(&pk); err != nil {
			panic(err.Error())
		}
	}
}
