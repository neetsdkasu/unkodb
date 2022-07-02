// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"reflect"
	"testing"
)

func TestParseStruct(t *testing.T) {

	type Food struct {
		Id    uint32 `unkodb:"id,key@Counter"`
		Name  string `unkodb:"name,ShortString"`
		Price int64  `unkodb:"price,Int64"`
	}

	foo := &Food{
		Id:    11,
		Name:  "カツカレー",
		Price: 800,
	}

	m, err := parseData(foo)
	if err != nil {
		t.Fatal(err)
	}

	data := make(tableTreeValue)
	data["id"] = CounterType(11)
	data["name"] = "カツカレー"
	data["price"] = int64(800)

	if !reflect.DeepEqual(m, data) {
		t.Fatalf("unmatch %#v, %#v", m, data)
	}

	_, err = parseData((*Food)(nil))
	if err != NotFoundData {
		t.Fatal(err)
	}

	boo := make(map[string]int)
	boo["id"] = 10
	boo["point"] = 120
	boo["count"] = 50
	if bm, err := parseData(boo); err != nil {
		t.Fatal(err)
	} else {
		mm := make(tableTreeValue)
		mm["id"] = 10
		mm["point"] = 120
		mm["count"] = 50
		if !reflect.DeepEqual(bm, mm) {
			t.Fatalf("unmatch %#v %#v", bm, mm)
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
