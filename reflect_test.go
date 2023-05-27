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

	m, err := parseData(nil, foo)
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

	_, err = parseData(nil, (*Food)(nil))
	if err != ErrNotFoundData {
		t.Fatal(err)
	}

	boo := make(map[string]int)
	boo["id"] = 10
	boo["point"] = 120
	boo["count"] = 50
	if bm, err := parseData(nil, boo); err != nil {
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
		CounterValue CounterType `unkodb:"id,key@Counter"`
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

	_, err = parseData(nil, hoge)
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

	err = createTableByTaggedStruct(tc, (*Food)(nil))
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
		data, err := parseData(table, food)
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
		e := fillDataToTaggedStruct(r, f)
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

	tableSpecList := []struct {
		Name string
		Spec any
	}{
		{"TestTable1", (*struct {
			Id    int8 `unkodb:"id,key@Int8"`
			Value int8 `unkodb:"value,Int8"`
		})(nil)},
		{"TestTable2", (*struct {
			Id    uint8 `unkodb:"id,key@Uint8"`
			Value uint8 `unkodb:"value,Uint8"`
		})(nil)},
		{"TestTable3", (*struct {
			Id    int16 `unkodb:"id,key@Int16"`
			Value int16 `unkodb:"value,Int16"`
		})(nil)},
		{"TestTable4", (*struct {
			Id    uint16 `unkodb:"id,key@Uint16"`
			Value uint16 `unkodb:"value,Uint16"`
		})(nil)},
		{"TestTable5", (*struct {
			Id    int32 `unkodb:"id,key@Int32"`
			Value int32 `unkodb:"value,Int32"`
		})(nil)},
		{"TestTable6", (*struct {
			Id    uint32 `unkodb:"id,key@Uint32"`
			Value uint32 `unkodb:"value,Uint32"`
		})(nil)},
		{"TestTable7", (*struct {
			Id    int64 `unkodb:"id,key@Int64"`
			Value int64 `unkodb:"value,Int64"`
		})(nil)},
		{"TestTable8", (*struct {
			Id    uint64 `unkodb:"id,key@Uint64"`
			Value uint64 `unkodb:"value,Uint64"`
		})(nil)},
		{"TestTable9", (*struct {
			Id    string `unkodb:"id,key@ShortString"`
			Value string `unkodb:"value,ShortString"`
		})(nil)},
		{"TestTable10", (*struct {
			Id    string `unkodb:"id,key@FixedSizeShortString[5]"`
			Value string `unkodb:"value,FixedSizeShortString[5]"`
		})(nil)},
		{"TestTable11", (*struct {
			Id    []byte `unkodb:"id,key@ShortBytes"`
			Value []byte `unkodb:"value,ShortBytes"`
		})(nil)},
		{"TestTable12", (*struct {
			Id    []byte `unkodb:"id,key@FixedSizeShortBytes[5]"`
			Value []byte `unkodb:"value,FixedSizeShortBytes[5]"`
		})(nil)},
	}

	for _, tableSpec := range tableSpecList {
		tc, err = db.CreateTable(tableSpec.Name)
		if err != nil {
			t.Fatal(err)
		}
		err = createTableByTaggedStruct(tc, tableSpec.Spec)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tc.Create()
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

func TestMoveDataToTaggedStruct(t *testing.T) {
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

	err = createTableByTaggedStruct(tc, (*Food)(nil))
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
		data, err := parseData(table, food)
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
		e := moveDataToTaggedStruct(r, f)
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

func TestParseDataStruct(t *testing.T) {
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
		Id    int    `unkodb:"id,key@Counter"`
		Name  string `unkodb:"name,ShortString"`
		Price int64  `unkodb:"price,Int64"`
	}

	err = createTableByTaggedStruct(tc, (*Food)(nil))
	if err != nil {
		t.Fatal(err)
	}

	table, err := tc.Create()
	if err != nil {
		t.Fatal(err)
	}

	list := []*Data{
		&Data{
			Key: CounterType(1),
			Columns: []any{
				"apple",
				int64(123),
			},
		},
		&Data{
			Key: CounterType(2),
			Columns: []any{
				"orange",
				int64(456),
			},
		},
		&Data{
			Key: CounterType(3),
			Columns: []any{
				"pine",
				int64(999),
			},
		},
	}

	for _, d := range list {
		m := parseDataStruct(table, d)
		if m == nil {
			t.Fatalf("empty %#v", d)
		}
		r, err := table.Insert(m)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(d.Key, r.Key()) {
			t.Fatalf("unmatch key %#v %#v", d.Key, r.Key())
		}
		if !reflect.DeepEqual(d.Columns, r.Columns()) {
			t.Fatalf("unmatch column name %#v %#v", d.Columns, r.Columns())
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

func TestMoveDataToDataStruct(t *testing.T) {
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
		Id    int    `unkodb:"id,key@Counter"`
		Name  string `unkodb:"name,ShortString"`
		Price int64  `unkodb:"price,Int64"`
	}

	err = createTableByTaggedStruct(tc, (*Food)(nil))
	if err != nil {
		t.Fatal(err)
	}

	table, err := tc.Create()
	if err != nil {
		t.Fatal(err)
	}

	data := make(tableTreeValue)
	data["id"] = CounterType(0)
	data["name"] = "Apple"
	data["price"] = int64(123)

	r, err := table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	var x *Data
	err = moveDataToDataStruct(r, &x)
	if err != nil {
		t.Fatal(err)
	}
	if x.Key.(CounterType) != 1 {
		t.Fatal("unmatch key")
	}
	if len(x.Columns) != 2 {
		t.Fatal("unmatch len")
	}
	if x.Columns[0].(string) != "Apple" {
		t.Fatal("unmatch name")
	}
	if x.Columns[1].(int64) != 123 {
		t.Fatal("unmatch price")
	}

	var y Data
	err = moveDataToDataStruct(r, &y)
	if err != nil {
		t.Fatal(err)
	}
	if y.Key.(CounterType) != 1 {
		t.Fatal("unmatch key")
	}
	if len(y.Columns) != 2 {
		t.Fatal("unmatch len")
	}
	if y.Columns[0].(string) != "Apple" {
		t.Fatal("unmatch name")
	}
	if y.Columns[1].(int64) != 123 {
		t.Fatal("unmatch price")
	}

	var z ***Data
	err = moveDataToDataStruct(r, &z)
	if err != errNotStruct {
		t.Fatal("not errNotStruct")
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

func TestFillDataToDataStruct(t *testing.T) {
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
		Id    int    `unkodb:"id,key@Counter"`
		Name  string `unkodb:"name,ShortString"`
		Price int64  `unkodb:"price,Int64"`
	}

	err = createTableByTaggedStruct(tc, (*Food)(nil))
	if err != nil {
		t.Fatal(err)
	}

	table, err := tc.Create()
	if err != nil {
		t.Fatal(err)
	}

	data := make(tableTreeValue)
	data["id"] = CounterType(0)
	data["name"] = "Apple"
	data["price"] = int64(123)

	r, err := table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	var x *Data
	err = fillDataToDataStruct(r, &x)
	if err != nil {
		t.Fatal(err)
	}
	if x.Key.(CounterType) != 1 {
		t.Fatal("unmatch key")
	}
	if len(x.Columns) != 2 {
		t.Fatal("unmatch len")
	}
	if x.Columns[0].(string) != "Apple" {
		t.Fatal("unmatch name")
	}
	if x.Columns[1].(int64) != 123 {
		t.Fatal("unmatch price")
	}

	var y Data
	err = fillDataToDataStruct(r, &y)
	if err != nil {
		t.Fatal(err)
	}
	if y.Key.(CounterType) != 1 {
		t.Fatal("unmatch key")
	}
	if len(y.Columns) != 2 {
		t.Fatal("unmatch len")
	}
	if y.Columns[0].(string) != "Apple" {
		t.Fatal("unmatch name")
	}
	if y.Columns[1].(int64) != 123 {
		t.Fatal("unmatch price")
	}

	var z ***Data
	err = fillDataToDataStruct(r, &z)
	if err != errNotStruct {
		t.Fatal("not errNotStruct")
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

func TestFillDataToTaggedStruct(t *testing.T) {
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
		Id    CounterType `unkodb:"id,key@Counter"`
		Name  string      `unkodb:"name,ShortString"`
		Price int64       `unkodb:"price,Int64"`
	}

	err = createTableByTaggedStruct(tc, (*Food)(nil))
	if err != nil {
		t.Fatal(err)
	}

	table, err := tc.Create()
	if err != nil {
		t.Fatal(err)
	}

	data := make(tableTreeValue)
	data["id"] = CounterType(0)
	data["name"] = "Apple"
	data["price"] = int64(123)

	r, err := table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	var x *Food
	err = fillDataToTaggedStruct(r, &x)
	if err != nil {
		t.Fatal(err)
	}
	if x.Id != 1 {
		t.Fatal("unmatch id")
	}
	if x.Name != "Apple" {
		t.Fatal("unmatch name")
	}
	if x.Price != 123 {
		t.Fatal("unmatch price")
	}

	var y Food
	err = fillDataToTaggedStruct(r, &y)
	if err != nil {
		t.Fatal(err)
	}
	if y.Id != 1 {
		t.Fatal("unmatch id")
	}
	if y.Name != "Apple" {
		t.Fatal("unmatch name")
	}
	if y.Price != 123 {
		t.Fatal("unmatch price")
	}

	var z ***Food
	err = fillDataToTaggedStruct(r, &z)
	if err != errNotStruct {
		t.Fatal("not errNotStruct")
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
