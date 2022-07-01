// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"testing"
)

func TestParseStruct(t *testing.T) {

	type Foo struct {
		Key   uint32 `unkodb:"id,key@Counter"`
		Name  string `unkodb:"name,ShortString"`
		Price int64  `unkodb:"price,Int64"`
	}

	foo := &Foo{
		Key:   11,
		Name:  "カツカレー",
		Price: 800,
	}

	m, err := parseStruct(foo)
	if err != nil {
		t.Fatal(err)
	}

	if id, ok := m["id"]; !ok {
		t.Fatal("not found id")
	} else if v, ok := id.(uint32); !ok {
		t.Fatal("wrong type id")
	} else if v != 11 {
		t.Fatal("wrong id value")
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
