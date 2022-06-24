// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUnkoDB(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	db, err := Create(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	tc, err := db.CreateTable("foodlist")
	if err != nil {
		t.Fatal(err)
	}

	err = tc.CounterKey("id")
	if err != nil {
		t.Fatal(err)
	}

	err = tc.ShortStringColumn("name")
	if err != nil {
		t.Fatal(err)
	}

	err = tc.Int64Column("price")
	if err != nil {
		t.Fatal(err)
	}

	table, err := tc.Create()
	if err != nil {
		t.Fatal(err)
	}

	// TODO write test
	data := make(map[string]any)
	data["id"] = uint32(0)
	data["name"] = "カツカレー"
	data["price"] = int64(500)
	err = table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	recs := []*Record{}
	err = table.IterateAll(func(r *Record) (_ bool) {
		recs = append(recs, r)
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(recs) != 1 {
		t.Fatalf("recs length is not 1 (%#v)", recs)
	}

	if id := recs[0].Key(); id == nil {
		t.Fatalf("invalid record %#v", recs[0])
	} else if id != any(uint32(1)) {
		t.Fatalf("invalid id %#v", id)
	}

	if id, ok := recs[0].Get("id"); !ok {
		t.Fatalf("invalid record %#v", recs[0])
	} else if id != any(uint32(1)) {
		t.Fatalf("invalid id %#v", id)
	}

	if id := recs[0].Column("id"); id == nil {
		t.Fatalf("invalid record %#v", recs[0])
	} else if id != any(uint32(1)) {
		t.Fatalf("invalid id %#v", id)
	}

	if name, ok := recs[0].Get("name"); !ok {
		t.Fatalf("invalid record %#v", recs[0])
	} else if name != any("カツカレー") {
		t.Fatalf("invalid name %#v", name)
	}

	if name := recs[0].Column("name"); name == nil {
		t.Fatalf("invalid record %#v", recs[0])
	} else if name != any("カツカレー") {
		t.Fatalf("invalid name %#v", name)
	}

	if price, ok := recs[0].Get("price"); !ok {
		t.Fatalf("invalid record %#v", recs[0])
	} else if price != any(int64(500)) {
		t.Fatalf("invalid price %#v", price)
	}

	if price := recs[0].Column("price"); price == nil {
		t.Fatalf("invalid record %#v", recs[0])
	} else if price != any(int64(500)) {
		t.Fatalf("invalid price %#v", price)
	}
	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
