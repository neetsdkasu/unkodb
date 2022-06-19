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

	err = tc.Int8Key("id")
	if err != nil {
		t.Fatal(err)
	}

	err = tc.ShortStringColumn("name")
	if err != nil {
		t.Fatal(err)
	}

	err = tc.Int8Column("price")
	if err != nil {
		t.Fatal(err)
	}

	table, err := tc.Create()
	if err != nil {
		t.Fatal(err)
	}

	// TODO write test
	_ = table

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
