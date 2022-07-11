// unkodb
// author: Leonardone @ NEETSDKASU

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

	err = table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
		f := &Food{}
		err := r.MoveTo(f)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[ITER] id: %d, name: %s, price: %d\n", f.Id, f.Name, f.Price)
		return
	})

	// Output:
	// [FIND] ID: 2, NAME: あんぱん, PRICE: 123
	// [ITER] id: 1, name: クリームパン, price: 234
	// [ITER] id: 2, name: あんぱん, price: 123
	// [ITER] id: 4, name: イチゴジャムパン, price: 987
	// [ITER] id: 5, name: 食パン, price: 333
}
