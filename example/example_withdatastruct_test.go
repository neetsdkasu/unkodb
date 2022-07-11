// unkodb
// author: Leonardone @ NEETSDKASU

package example

import (
	"fmt"
	"log"
	"os"

	"github.com/neetsdkasu/unkodb"
)

func ExampleUnkoDB_withDataStruct() {

	// Exampleのテスト用のファイルなのでテスト実行後は削除する･･･
	const FileExists = false
	file, err := os.CreateTemp("", "example.unkodb")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())

	var (
		db    *unkodb.UnkoDB
		table *unkodb.Table
	)

	if FileExists {
		// file, err := os.OpenFile("example.unkodb", os.O_RDWR, 0755)
		// defer file.Close()

		db, err = unkodb.Open(file)
		if err != nil {
			log.Fatal(err)
		}

		table, err = db.Table("food_table")
		if err != nil {
			log.Fatal(err)
		}

	} else {
		// file, err := os.Create("example.unkodb")
		// defer file.Close()

		db, err = unkodb.Create(file)
		if err != nil {
			log.Fatal(err)
		}

		tc, err := db.CreateTable("food_table")
		if err != nil {
			log.Fatal(err)
		}

		tc.CounterKey("id")
		tc.ShortStringColumn("name")
		tc.Int64Column("price")

		table, err = tc.Create()
		if err != nil {
			log.Fatal(err)
		}

	}

	list := []*unkodb.Data{
		&unkodb.Data{
			Key:     unkodb.CounterType(0),
			Columns: []any{"クリームパン", int64(234)},
		},
		&unkodb.Data{
			Key:     unkodb.CounterType(0),
			Columns: []any{"あんぱん", int64(123)},
		},
		&unkodb.Data{
			Key:     unkodb.CounterType(0),
			Columns: []any{"カレーパン", int64(345)},
		},
		&unkodb.Data{
			Key:     unkodb.CounterType(0),
			Columns: []any{"ジャムパン", int64(222)},
		},
		&unkodb.Data{
			Key:     unkodb.CounterType(0),
			Columns: []any{"食パン", int64(333)},
		},
	}

	for _, item := range list {
		_, err = table.Insert(item)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Find id=2 あんぱん
	r, err := table.Find(unkodb.CounterType(2))
	if err != nil {
		log.Fatal(err)
	}
	var food unkodb.Data
	err = r.MoveTo(&food)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[FIND] ID: %d, NAME: %s, PRICE: %d\n", food.Key, food.Columns[0], food.Columns[1])

	// Delete id=3 カレーパン
	err = table.Delete(unkodb.CounterType(3))
	if err != nil {
		log.Fatal(err)
	}

	// Replace id=4 ジャムパン
	replace := &unkodb.Data{
		Key:     unkodb.CounterType(4),
		Columns: []any{"イチゴジャムパン", int64(987)},
	}
	_, err = table.Replace(replace)
	if err != nil {
		log.Fatal(err)
	}

	err = table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
		f := &unkodb.Data{}
		err := r.MoveTo(f)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[ITER] id: %d, name: %s, price: %d\n", f.Key, f.Columns[0], f.Columns[1])
		return
	})

	// Output:
	// [FIND] ID: 2, NAME: あんぱん, PRICE: 123
	// [ITER] id: 1, name: クリームパン, price: 234
	// [ITER] id: 2, name: あんぱん, price: 123
	// [ITER] id: 4, name: イチゴジャムパン, price: 987
	// [ITER] id: 5, name: 食パン, price: 333
}
