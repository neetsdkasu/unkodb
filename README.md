# unkodb

データをファイルに書き込む感じのやつです。  
ＤＢではないですしＫＶＳとやらとも違うと思います。  



### 例

##### `map[string]any`でデータを取り扱う
キー名やカラム名をマップのキーとして各値を保持する。（値の型に注意する必要がある）
```go
package example

import (
	"fmt"
	"log"
	"os"

	"github.com/neetsdkasu/unkodb"
)

func ExampleUnkoDB() {

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

	}

	// Find id=2 あんぱん
	r, err := table.Find(unkodb.CounterType(2))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[FIND] ID: %d, NAME: %s, PRICE: %d\n", r.Key(), r.Column("name"), r.Column("price"))

	// Iteration データを順番に辿る
	err = table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
		fmt.Printf("[ITER] id: %d, name: %s, price: %d\n", r.Column("id"), r.Column("name"), r.Column("price"))
		return
	})
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// [FIND] ID: 2, NAME: あんぱん, PRICE: 123
	// [ITER] id: 1, name: クリームパン, price: 234
	// [ITER] id: 2, name: あんぱん, price: 123
	// [ITER] id: 4, name: イチゴジャムパン, price: 987
	// [ITER] id: 5, name: 食パン, price: 333
}
```


##### `unkodb.Data`でデータを取り扱う
キーの値とカラムの値だけを保持する`unkodb.Data`を用いる。（値の型に注意する必要がある）
```go
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

	// Iteration データを順番に辿る
	err = table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
		f := &unkodb.Data{}
		err := r.MoveTo(f)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[ITER] id: %d, name: %s, price: %d\n", f.Key, f.Columns[0], f.Columns[1])
		return
	})
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// [FIND] ID: 2, NAME: あんぱん, PRICE: 123
	// [ITER] id: 1, name: クリームパン, price: 234
	// [ITER] id: 2, name: あんぱん, price: 123
	// [ITER] id: 4, name: イチゴジャムパン, price: 987
	// [ITER] id: 5, name: 食パン, price: 333
}
```

##### `unkodb`のタグをつけた構造体でデータを取り扱う
ユーザ定義の型の各フィールドにタグ`unkodb`を振る。カラム名とカラム型を記述する必要がある。キーとなるフィールドのカラム型には`key@`プリフィクスをつける。
```go
package example

import (
	"fmt"
	"log"
	"os"

	"github.com/neetsdkasu/unkodb"
)

func ExampleUnkoDB_withTaggedStruct() {

	// Exampleのテスト用のファイルなのでテスト実行後は削除する･･･
	const FileExists = false
	file, err := os.CreateTemp("", "example.unkodb")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())

	type Food struct {
		Id    unkodb.CounterType `unkodb:"id,key@Counter"`
		Name  string             `unkodb:"name,ShortString"`
		Price int64              `unkodb:"price,Int64"`
	}

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

		table, err = db.CreateTableByTaggedStruct("food_table", (*Food)(nil))
		if err != nil {
			log.Fatal(err)
		}

		list := []*Food{
			&Food{
				Name:  "クリームパン",
				Price: 234,
			},
			&Food{
				Name:  "あんぱん",
				Price: 123,
			},
			&Food{
				Name:  "カレーパン",
				Price: 345,
			},
			&Food{
				Name:  "ジャムパン",
				Price: 222,
			},
			&Food{
				Name:  "食パン",
				Price: 333,
			},
		}

		for _, item := range list {
			_, err = table.Insert(item)
			if err != nil {
				log.Fatal(err)
			}
		}

		// Delete id=3 カレーパン
		err = table.Delete(unkodb.CounterType(3))
		if err != nil {
			log.Fatal(err)
		}

		// Replace id=4 ジャムパン
		replace := &Food{
			Id:    4,
			Name:  "イチゴジャムパン",
			Price: 987,
		}
		_, err = table.Replace(replace)
		if err != nil {
			log.Fatal(err)
		}

	}

	// Find id=2 あんぱん
	r, err := table.Find(unkodb.CounterType(2))
	if err != nil {
		log.Fatal(err)
	}
	var food Food
	err = r.MoveTo(&food)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[FIND] ID: %d, NAME: %s, PRICE: %d\n", food.Id, food.Name, food.Price)

	// Iteration データを順番に辿る
	err = table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
		f := &Food{}
		err := r.MoveTo(f)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[ITER] id: %d, name: %s, price: %d\n", f.Id, f.Name, f.Price)
		return
	})
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// [FIND] ID: 2, NAME: あんぱん, PRICE: 123
	// [ITER] id: 1, name: クリームパン, price: 234
	// [ITER] id: 2, name: あんぱん, price: 123
	// [ITER] id: 4, name: イチゴジャムパン, price: 987
	// [ITER] id: 5, name: 食パン, price: 333
}
```