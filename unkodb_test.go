// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/neetsdkasu/avltree"
)

func TestUnkoDB_CreateTableByTaggedStruct(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	db, err := Create(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	type Food struct {
		Id    CounterType `unkodb:"id,key@Counter"`
		Name  string      `unkodb:"name,ShortString"`
		Price int64       `unkodb:"price,Int64"`
	}

	table, err := db.CreateTableByTaggedStruct("foodlist", (*Food)(nil))
	if err != nil {
		t.Fatal(err)
	}

	list := []*Food{
		&Food{
			Name:  "りんご",
			Price: 500,
		},
		&Food{
			Name:  "ソフトクリーム",
			Price: 800,
		},
		&Food{
			Name:  "カレーライス",
			Price: 1200,
		},
	}

	for i, item := range list {
		r, err := table.Insert(item)
		if err != nil {
			t.Fatal(err)
		}
		var f Food
		err = r.MoveTo(&f)
		if err != nil {
			t.Fatal(err)
		}
		if f.Id != CounterType(i+1) {
			t.Fatalf("unmatch id %#v %#v", f, item)
		}
		if f.Name != item.Name {
			t.Fatalf("unmatch name %#v %#v", f, item)
		}
		if f.Price != item.Price {
			t.Fatalf("unmatch price %#v %#v", f, item)
		}

		f.Price *= 2

		r, err = table.Replace(&f)
		if err != nil {
			t.Fatal(err)
		}

		var g Food
		err = r.CopyTo(&g)
		if err != nil {
			t.Fatal(err)
		}
		if g != f {
			t.Fatalf("unmatch %#v %#v", g, f)
		}
	}

	result := []*Food{}

	err = table.IterateAll(func(r *Record) (_ bool) {
		f := &Food{}
		err = r.MoveTo(f)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, f)
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != len(list) {
		t.Fatal("unmatch list size")
	}

	for i, item := range list {
		res := result[i]
		if res.Id != CounterType(i+1) {
			t.Fatal("unmatch id")
		}
		if res.Name != item.Name {
			t.Fatal("unmatch name")
		}
		if res.Price != item.Price*2 {
			t.Fatal("unmatch price")
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

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
	data["price"] = int64(800)
	_, err = table.Insert(data)
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
	} else if price != any(int64(800)) {
		t.Fatalf("invalid price %#v", price)
	}

	if price := recs[0].Column("price"); price == nil {
		t.Fatalf("invalid record %#v", recs[0])
	} else if price != any(int64(800)) {
		t.Fatalf("invalid price %#v", price)
	}

	tc, err = db.CreateTable("ゲームリスト")
	if err != nil {
		t.Fatal(err)
	}
	err = tc.ShortStringKey("ゲームタイトル")
	if err != nil {
		t.Fatal(err)
	}
	err = tc.FixedSizeLongStringColumn("ハード", 5)
	if err != nil {
		t.Fatal(err)
	}
	err = tc.Float64Column("好き具合")
	if err != nil {
		t.Fatal(err)
	}
	table, err = tc.Create()
	if err != nil {
		t.Fatal(err)
	}

	data = make(map[string]any)
	data["ゲームタイトル"] = "バイオハザード"
	data["ハード"] = "PS"
	data["好き具合"] = -1.234
	_, err = table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}
	data["ゲームタイトル"] = "ドラクエ5"
	data["ハード"] = "SFC"
	data["好き具合"] = 123.4
	_, err = table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}
	data["ゲームタイトル"] = "スペランカー"
	data["ハード"] = "FC"
	data["好き具合"] = 5.4
	_, err = table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	if table.Count() != 3 {
		t.Fatalf("wrong record count %d", table.Count())
	}

	if id, err := table.NextCounterID(); err != KeyIsNotCounter || id != 0 {
		t.Fatalf("wrong next id %v %v", err, id)
	}

	table, err = db.Table("foodlist")
	if err != nil {
		t.Fatal(err)
	}

	data["id"] = uint32(0)
	data["name"] = "コロッケカレー"
	data["price"] = int64(600)
	_, err = table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	data["id"] = uint32(0)
	data["name"] = "チャーシュー麺"
	data["price"] = int64(700)
	_, err = table.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	db2, err := Open(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	table2, err := db2.Table("foodlist")
	if err != nil {
		t.Fatal(err)
	}

	data = make(map[string]any)
	data["id"] = uint32(0)
	data["name"] = "ざるそば"
	data["price"] = int64(400)
	_, err = table2.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	recs2 := []*Record{}
	err = table2.IterateAll(func(r *Record) (_ bool) {
		recs2 = append(recs2, r)
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(recs2) != 4 {
		t.Fatalf("recs length is not 4 (%#v)", recs2)
	}

	if id := recs2[0].Key(); id == nil {
		t.Fatalf("invalid record %#v", recs2[0])
	} else if id != any(uint32(1)) {
		t.Fatalf("invalid id %#v", id)
	}

	if name := recs2[0].Column("name"); name == nil {
		t.Fatalf("invalid record %#v", recs2[0])
	} else if name != any("カツカレー") {
		t.Fatalf("invalid name %#v", name)
	}

	if price := recs2[0].Column("price"); price == nil {
		t.Fatalf("invalid record %#v", recs2[0])
	} else if price != any(int64(800)) {
		t.Fatalf("invalid price %#v", price)
	}

	if id := recs2[1].Key(); id == nil {
		t.Fatalf("invalid record %#v", recs2[1])
	} else if id != any(uint32(2)) {
		t.Fatalf("invalid id %#v", id)
	}

	if name := recs2[1].Column("name"); name == nil {
		t.Fatalf("invalid record %#v", recs2[1])
	} else if name != any("コロッケカレー") {
		t.Fatalf("invalid name %#v", name)
	}

	if price := recs2[1].Column("price"); price == nil {
		t.Fatalf("invalid record %#v", recs2[1])
	} else if price != any(int64(600)) {
		t.Fatalf("invalid price %#v", price)
	}

	if id := recs2[2].Key(); id == nil {
		t.Fatalf("invalid record %#v", recs2[2])
	} else if id != any(uint32(3)) {
		t.Fatalf("invalid id %#v", id)
	}

	if name := recs2[2].Column("name"); name == nil {
		t.Fatalf("invalid record %#v", recs2[2])
	} else if name != any("チャーシュー麺") {
		t.Fatalf("invalid name %#v", name)
	}

	if price := recs2[2].Column("price"); price == nil {
		t.Fatalf("invalid record %#v", recs2[2])
	} else if price != any(int64(700)) {
		t.Fatalf("invalid price %#v", price)
	}

	if id := recs2[3].Key(); id == nil {
		t.Fatalf("invalid record %#v", recs2[3])
	} else if id != any(uint32(4)) {
		t.Fatalf("invalid id %#v", id)
	}

	if name := recs2[3].Column("name"); name == nil {
		t.Fatalf("invalid record %#v", recs2[3])
	} else if name != any("ざるそば") {
		t.Fatalf("invalid name %#v", name)
	}

	if price := recs2[3].Column("price"); price == nil {
		t.Fatalf("invalid record %#v", recs2[3])
	} else if price != any(int64(400)) {
		t.Fatalf("invalid price %#v", price)
	}

	table2, err = db2.Table("ゲームリスト")
	if err != nil {
		t.Fatal(err)
	}

	if table2.Count() != 3 {
		t.Fatalf("wrong record count %d", table.Count())
	}

	if id, err := table2.NextCounterID(); err != KeyIsNotCounter || id != 0 {
		t.Fatalf("wrong next id %v %v", err, id)
	}

	text := ""
	err = table2.IterateAll(func(r *Record) (_ bool) {
		text += r.Key().(string)
		text += r.Column("ハード").(string)
		text += fmt.Sprint(r.Column("好き具合").(float64))
		return
	})
	if err != nil {
		t.Fatal(err)
	}

	if text != "スペランカーFC   5.4ドラクエ5SFC  123.4バイオハザードPS   -1.234" {
		t.Fatal(text)
	}

	table2, err = db2.Table("foodlist")
	if err != nil {
		t.Fatal(err)
	}

	rec, err := table2.Find(CounterType(2))
	if err != nil {
		t.Fatal(err)
	}

	if id := rec.Key(); id == nil {
		t.Fatalf("invalid record %#v", rec)
	} else if id != any(uint32(2)) {
		t.Fatalf("invalid id %#v", id)
	}

	if name := rec.Column("name"); name == nil {
		t.Fatalf("invalid record %#v", rec)
	} else if name != any("コロッケカレー") {
		t.Fatalf("invalid name %#v", name)
	}

	if price := rec.Column("price"); price == nil {
		t.Fatalf("invalid record %#v", rec)
	} else if price != any(int64(600)) {
		t.Fatalf("invalid price %#v", price)
	}

	data["id"] = CounterType(2)
	data["name"] = "カツサンド"
	data["price"] = int64(500)
	_, err = table2.Replace(data)
	if err != nil {
		t.Fatal(err)
	}

	data["id"] = CounterType(10)
	data["name"] = "おにぎり"
	data["price"] = int64(100)
	_, err = table2.Replace(data)
	if err != NotFoundKey {
		t.Fatal("not NotFoundKey", err)
	}

	rec, err = table2.Find(CounterType(2))
	if err != nil {
		t.Fatal(err)
	}

	if id := rec.Key(); id == nil {
		t.Fatalf("invalid record %#v", rec)
	} else if id != any(uint32(2)) {
		t.Fatalf("invalid id %#v", id)
	}

	if name := rec.Column("name"); name == nil {
		t.Fatalf("invalid record %#v", rec)
	} else if name != any("カツサンド") {
		t.Fatalf("invalid name %#v", name)
	}

	if price := rec.Column("price"); price == nil {
		t.Fatalf("invalid record %#v", rec)
	} else if price != any(int64(500)) {
		t.Fatalf("invalid price %#v", price)
	}

	if table2.Count() != 4 {
		t.Fatalf("table2.Count is not 4 (%d)", table2.Count())
	}

	if id, err := table2.NextCounterID(); err != nil || id != 5 {
		t.Fatalf("wrong next id %v %v", err, id)
	}

	err = table2.Delete(CounterType(3))
	if err != nil {
		t.Fatal(err)
	}

	err = table2.Delete(CounterType(3))
	if err != NotFoundKey {
		t.Fatalf("wrong delete %v", err)
	}

	if table2.Count() != 3 {
		t.Fatalf("table2.Count is not 3 (%d)", table2.Count())
	}

	if id, err := table2.NextCounterID(); err != nil || id != 5 {
		t.Fatalf("wrong next id %v %v", err, id)
	}

	db3, err := Open(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	table3, err := db3.Table("foodlist")
	if err != nil {
		t.Fatal(err)
	}

	recs3 := []*Record{}
	err = table3.IterateAll(func(r *Record) (_ bool) {
		recs3 = append(recs3, r)
		return
	})

	if len(recs3) != 3 {
		t.Fatalf("wrong record count %#v", recs3)
	}

	text3 := ""
	for _, r := range recs3 {
		text3 += fmt.Sprint(r.Key(), r.Column("name"), r.Column("price"))
	}

	if text3 != "1カツカレー8002カツサンド5004ざるそば400" {
		t.Fatal(text3)
	}

	if c := avltree.Count(db3.segManager.tree); c != 1 {
		t.Fatalf("wrong idle tree count %d", c)
	}

	data["id"] = CounterType(0)
	data["name"] = "明太ピザ"
	data["price"] = int64(1200)
	_, err = table3.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	data["id"] = CounterType(0)
	data["name"] = "チーズインハンバーグ"
	data["price"] = int64(1200)
	_, err = table3.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	if c := avltree.Count(db3.segManager.tree); c != 1 {
		t.Fatalf("wrong idle tree count %d", c)
	}

	data["id"] = CounterType(0)
	data["name"] = "明太子おにぎり"
	data["price"] = int64(200)
	_, err = table3.Insert(data)
	if err != nil {
		t.Fatal(err)
	}

	if c := avltree.Count(db3.segManager.tree); c != 0 {
		t.Fatalf("wrong idle tree count %d", c)
	}

	rec, err = table3.Find(CounterType(7))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Column("name").(string) != "明太子おにぎり" {
		t.Fatal(rec.data)
	}

	text3 = ""
	err = table3.IterateRange(CounterType(3), CounterType(5), func(r *Record) (_ bool) {
		text3 += fmt.Sprint(r.Key(), r.Column("name"), r.Column("price"))
		return
	})

	if text3 != "4ざるそば4005明太ピザ1200" {
		t.Fatal(text3)
	}

	text3 = ""
	err = table3.IterateRange(nil, CounterType(3), func(r *Record) (_ bool) {
		text3 += fmt.Sprint(r.Key(), r.Column("name"), r.Column("price"))
		return
	})

	if text3 != "1カツカレー8002カツサンド500" {
		t.Fatal(text3)
	}

	text3 = ""
	err = table3.IterateRange(CounterType(5), nil, func(r *Record) (_ bool) {
		text3 += fmt.Sprint(r.Key(), r.Column("name"), r.Column("price"))
		return
	})

	if text3 != "5明太ピザ12006チーズインハンバーグ12007明太子おにぎり200" {
		t.Fatal(text3)
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

func TestUnkoDB_openFile(t *testing.T) {
	tempfileName := filepath.Join(t.TempDir(), "test.unkodb")

	type Food struct {
		Id    CounterType `unkodb:"id,key@Counter"`
		Name  string      `unkodb:"name,ShortString"`
		Price int64       `unkodb:"price,Int64"`
	}

	list := []*Food{
		&Food{Name: "ハンバーガー", Price: 250},
		&Food{Name: "サンドイッチ", Price: 200},
		&Food{Name: "おにぎり", Price: 130},
		&Food{Name: "お弁当", Price: 600},
		&Food{Name: "グラタン", Price: 800},
	}

	{
		tempfile, err := os.Create(tempfileName)
		if err != nil {
			t.Fatal(err)
		}
		defer tempfile.Close()

		db, err := Create(tempfile)
		if err != nil {
			t.Fatal(err)
		}

		table, err := db.CreateTableByTaggedStruct("foodlist", (*Food)(nil))
		if err != nil {
			t.Fatal(err)
		}

		for _, item := range list {
			_, err = table.Insert(item)
			if err != nil {
				t.Fatal(err)
			}
		}

		err = table.Delete(CounterType(3))
		if err != nil {
			t.Fatal(err)
		}

		err = tempfile.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	list1 := []*Food{
		&Food{Name: "アイスクリーム", Price: 180},
		&Food{Name: "板チョコ", Price: 150},
		&Food{Name: "キャンディー", Price: 100},
		&Food{Name: "ポテチ", Price: 190},
		&Food{Name: "せんべい", Price: 240},
	}

	{
		openfile, err := os.OpenFile(tempfileName, os.O_RDWR, 0755)
		if err != nil {
			t.Fatal(err)
		}
		defer openfile.Close()

		db1, err := Open(openfile)
		if err != nil {
			t.Fatal(err)
		}

		table1, err := db1.Table("foodlist")
		if err != nil {
			t.Fatal(err)
		}

		result1 := []*Food{}
		err = table1.IterateAll(func(r *Record) (_ bool) {
			f := &Food{}
			err = r.MoveTo(f)
			if err != nil {
				t.Fatal(err)
			}
			result1 = append(result1, f)
			return
		})
		if err != nil {
			t.Fatal(err)
		}

		if len(result1) != 4 {
			t.Fatal("unmatch length result")
		}

		for _, item := range list1 {
			_, err = table1.Insert(item)
			if err != nil {
				t.Fatal(err)
			}
		}

		err = openfile.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	{
		// os.Openはリードオンリー
		openfile2, err := os.Open(tempfileName)
		if err != nil {
			t.Fatal(err)
		}
		defer openfile2.Close()

		db2, err := Open(openfile2)
		if err != nil {
			t.Fatal(err)
		}

		table2, err := db2.Table("foodlist")
		if err != nil {
			t.Fatal(err)
		}

		result2 := []*Food{}
		err = table2.IterateAll(func(r *Record) (_ bool) {
			f := &Food{}
			err = r.MoveTo(f)
			if err != nil {
				t.Fatal(err)
			}
			result2 = append(result2, f)
			return
		})
		if err != nil {
			t.Fatal(err)
		}

		if len(result2) != 9 {
			t.Fatal("unmatch length result")
		}

		list2 := append(append(append([]*Food{}, list[:2]...), list[3:]...), list1...)

		for i, item := range list2 {
			if i < 2 {
				if result2[i].Id != CounterType(i+1) {
					t.Fatal("unmatch id")
				}
			} else {
				if result2[i].Id != CounterType(i+2) {
					t.Fatal("unmatch id")
				}
			}
			if item.Name != result2[i].Name {
				t.Fatal("unmatch name")
			}
			if item.Price != result2[i].Price {
				t.Fatal("unmatch price")
			}
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
