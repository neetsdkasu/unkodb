// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestParseStruct(t *testing.T) {

	type Food struct {
		Id    CounterType `unkodb:"id,key@Counter"`
		Name  string      `unkodb:"name,ShortString"`
		Price int64       `unkodb:"price,Int64"`
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
		CounterValue CounterType `unkodb:"key@Counter"`
		Int8value    int8        `unkodb:"i8,Int8"`
		Int16value   int16       `unkodb:"i16,Int16"`
		Int32value   int32       `unkodb:"i32,Int32"`
		Int64value   int64       `unkodb:"i64,Int64"`
		Uint8value   uint8       `unkodb:"u8,Uint8"`
		Uint16value  uint16      `unkodb:"u16,Uint16"`
		Uint32value  uint32      `unkodb:"u32,Uint32"`
		Uint64value  uint64      `unkodb:"u64,Uint64"`
		Float32value float32     `unkodb:"f32,Float32"`
		Float64value float64     `unkodb:"f64,Float64"`
		SSvalue      string      `unkodb:"ss,ShortString"`
		FSSSvalue    string      `unkodb:"fsss,FixedSizeShortString[100]"`
		LSvalue      string      `unkodb:"ls,LongString"`
		FSLSvalue    string      `unkodb:"fsls,FixedSizeLongString[300]"`
		Text         string      `unkodb:"tx,Text"`
		SBvalue      []byte      `unkodb:"sb,ShortBytes"`
		FSSBvalue    [20]byte    `unkodb:"fssb,FixedSizeShortBytes[20]"`
		LBvalue      []byte      `unkodb:"lb,LongBytes"`
		FSLBvalue    [300]byte   `unkodb:"fslb,FixedSizeLongBytes[300]"`
		Blob         []byte      `unkodb:"bl,Blob"`
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
		Id    int     `unkodb:"id,key@Counter"`
		Name  string  `unkodb:"name,ShortString"`
		Price int64   `unkodb:"price,Int64"`
		B1    []byte  `unkodb:"b1,ShortBytes"`
		B2    [3]byte `unkodb:"b2,FixedSizeShortBytes[3]"`
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
			B1:    []byte{1, 0, 3, 0, 4, 0},
			B2:    [3]byte{0, 3, 1},
		},
		&Food{
			Name:  "からあげ",
			Price: 150,
			B1:    []byte{1, 0, 5, 0, 6, 0},
			B2:    [3]byte{0, 5, 1},
		},
		&Food{
			Name:  "みかんゼリー",
			Price: 160,
			B1:    []byte{1, 0, 6, 0, 7, 0},
			B2:    [3]byte{0, 6, 1},
		},
		&Food{
			Name:  "ヨーグルト",
			Price: 140,
			B1:    []byte{1, 0, 4, 0, 5, 0},
			B2:    [3]byte{0, 4, 1},
		},
		&Food{
			Name:  "板チョコ",
			Price: 120,
			B1:    []byte{1, 0, 2, 0, 3, 0},
			B2:    [3]byte{0, 2, 1},
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

	results := []*Food{}

	err = table.IterateAll(func(r *Record) (_ bool) {
		f := &Food{}
		e := fillDataByTag(r, f)
		if e != nil {
			t.Fatal(e)
		}
		results = append(results, f)
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(list) != len(results) {
		t.Fatalf("invalid length %d %d", len(list), len(results))
	}

	for i, res := range results {
		if i+1 != res.Id {
			t.Fatalf("invalid id %d %#v", i+1, res)
		}
		if list[i].Name != res.Name {
			t.Fatalf("unmatch Name %s %#v", list[i].Name, res)
		}
		if list[i].Price != res.Price {
			t.Fatalf("unmatch Price %d %#v", list[i].Price, res)
		}
		if !bytes.Equal(list[i].B1, res.B1) {
			t.Fatalf("unmatch B1 %v %#v", list[i].B1, res)
		}
		if !bytes.Equal(list[i].B2[:], res.B2[:]) {
			t.Fatalf("unmatch B2 %v %#v", list[i].B2, res)
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
