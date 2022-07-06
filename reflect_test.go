// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"os"
	"path/filepath"
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

	type Hoge struct {
		CounterValue uint32    `unkodb:"key@Counter"`
		Int8value    int8      `unkodb:"i8,Int8"`
		Int16value   int16     `unkodb:"i16,Int16"`
		Int32value   int32     `unkodb:"i32,Int32"`
		Int64value   int64     `unkodb:"i64,Int64"`
		Uint8value   uint8     `unkodb:"u8,Uint8"`
		Uint16value  uint16    `unkodb:"u16,Uint16"`
		Uint32value  uint32    `unkodb:"u32,Uint32"`
		Uint64value  uint64    `unkodb:"u64,Uint64"`
		Float32value float32   `unkodb:"f32,Float32"`
		Float64value float64   `unkodb:"f64,Float64"`
		SSvalue      string    `unkodb:"ss,ShortString"`
		FSSSvalue    string    `unkodb:"fsss,FixedSizeShortString[100]"`
		LSvalue      string    `unkodb:"ls,LongString"`
		FSLSvalue    string    `unkodb:"fsls,FixedSizeLongString[300]"`
		Text         string    `unkodb:"tx,Text"`
		SBvalue      []byte    `unkodb:"sb,ShortBytes"`
		FSSBvalue    [20]byte  `unkodb:"fssb,FixedSizeShortBytes[20]"`
		LBvalue      []byte    `unkodb:"lb,LongBytes"`
		FSLBvalue    [300]byte `unkodb:"fslb,FixedSizeLongBytes[300]"`
		Blob         []byte    `unkodb:"bl,Blob"`
	}

	hoge := &Hoge{}

	_, err = parseData(hoge)
	if err != nil {
		t.Fatal(err)
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

func TestCreateTableByTag(t *testing.T) {
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

	type Food struct {
		Id    uint32 `unkodb:"id,key@Counter"`
		Name  string `unkodb:"name,ShortString"`
		Price int64  `unkodb:"price,Int64"`
	}

	err = createTableByTag(tc, (*Food)(nil))
	if err != nil {
		t.Fatal(err)
	}

	table, err := tc.Create()
	if err != nil {
		t.Fatal(err)
	}

	list := []*Food{
		&Food{
			Name:  "コロッケ",
			Price: 130,
		},
		&Food{
			Name:  "からあげ",
			Price: 150,
		},
		&Food{
			Name:  "みかんゼリー",
			Price: 160,
		},
		&Food{
			Name:  "ヨーグルト",
			Price: 140,
		},
		&Food{
			Name:  "板チョコ",
			Price: 120,
		},
	}

	for _, food := range list {
		data, err := parseData(food)
		if err != nil {
			t.Fatal(err)
		}
		_, err = table.Insert(data)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
