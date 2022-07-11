package example

import (
	"fmt"
	"log"
	"os"

	"github.com/neetsdkasu/unkodb"
)

func ExampleUnkoDB() {
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
		// if UnkoDB file exists

		db, err = unkodb.Open(file)
		if err != nil {
			log.Fatal(err)
		}

		table, err = db.Table("food_table")
		if err != nil {
			log.Fatal(err)
		}

	} else {

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

	list := []map[string]any{
		map[string]any{
			"id":    unkodb.CounterType(0),
			"name":  "クリームパン",
			"price": int64(234),
		},
		map[string]any{
			"id":    unkodb.CounterType(0),
			"name":  "あんぱん",
			"price": int64(123),
		},
		map[string]any{
			"id":    unkodb.CounterType(0),
			"name":  "カレーパン",
			"price": int64(345),
		},
		map[string]any{
			"id":    unkodb.CounterType(0),
			"name":  "ジャムパン",
			"price": int64(222),
		},
		map[string]any{
			"id":    unkodb.CounterType(0),
			"name":  "食パン",
			"price": int64(333),
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
	fmt.Printf("[FIND] ID: %d, NAME: %s, PRICE: %d\n", r.Key(), r.Column("name"), r.Column("price"))

	// Delete id=3 カレーパン
	err = table.Delete(unkodb.CounterType(3))
	if err != nil {
		log.Fatal(err)
	}

	// Replace id=4 ジャムパン
	replace := map[string]any{
		"id":    unkodb.CounterType(4),
		"name":  "イチゴジャムパン",
		"price": int64(987),
	}
	_, err = table.Replace(replace)
	if err != nil {
		log.Fatal(err)
	}

	err = table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
		fmt.Printf("[ITER] id: %d, name: %s, price: %d\n", r.Column("id"), r.Column("name"), r.Column("price"))
		return
	})

	// Output:
	// [FIND] ID: 2, NAME: あんぱん, PRICE: 123
	// [ITER] id: 1, name: クリームパン, price: 234
	// [ITER] id: 2, name: あんぱん, price: 123
	// [ITER] id: 4, name: イチゴジャムパン, price: 987
	// [ITER] id: 5, name: 食パン, price: 333
}
