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

	err = tc.Int32Column("price")
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
	data["name"] = "apple"
	data["price"] = int32(500)
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

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
