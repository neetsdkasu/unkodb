# unkodb

データを単一ファイルに読み書きする感じ？    
ＤＢではないです。


-------------------------------------------------------------------------------------


### 説明？

 - ファイルサイズは2GB以下までしか扱えない
 - ファイルに対しては直接の操作ではなくインターフェース（`io.ReadWriteSeeker`）越しの読み書きしか行わない（共有ロックや`Flush`や`Close`などの処理等は呼び出し側のほうで行う必要がある）
 - データのサイズの変わる更新や削除を行うと使用できないゴミ領域が発生するが対処はしてない
 - テーブルの名前やカラムを変える仕組みは無い
 - 無駄なIO処理やメモリ確保が多いため大量のデータの取り扱いや頻繁なアクセスには向いてない
 - トランザクションのような仕組みは無い
 - スレッドセーフではない
 - フェイルセーフではない
 - ファイルフォーマットを確認しないため不正なファイル読み込みでパニックするかも
 - テーブル名とカラム名は1バイト以上255バイト以下で指定する必要がある（Goのstringを[]byteにキャストした際のサイズ）
 - テーブル名とカラム名に使える文字は今のところ制限は設けていない
 - カラム数はテーブルごとに100個まで
 - 内部的にはAVL木で管理されている（AVL木の実装が正しければよいが･･･）
 - 各テーブルにキーを１つ指定する
 - データの検索はキーでのみ行える（キーの重複は許されてない）
 - デバッグ不十分なのでバグだらけなのでバグでデータが破壊される可能性が高いです（死）



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

		table = db.Table("food_table")
		if table == nil {
			log.Fatal("not found food_table")
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

		// Insert
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

		table = db.Table("food_table")
		if table == nil {
			log.Fatal("not found food_table")
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

		// Insert
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

		table = db.Table("food_table")
		if table == nil {
			log.Fatal("not found food_table")
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

		// Insert
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



### カラムの型

| カラムの型           | キー | カラム | Goの型  | 備考                                                                                                                           |
|:---------------------|:----:|:------:|:--------|:-------------------------------------------------------------------------------------------------------------------------------|
| Counter              | ○   | －     | uint32  | データが挿入時に値が設定される。挿入ごとに1ずつ値が増えていく（最初は1から始まる）。`unkodb.CounterType`はuint32のエイリアス。 |
| Int8                 | ○   | ○     | int8    |                                                                                                                                |
| Int16                | ○   | ○     | int16   |                                                                                                                                |
| Int32                | ○   | ○     | int32   |                                                                                                                                |
| Int64                | ○   | ○     | int64   |                                                                                                                                |
| Uint8                | ○   | ○     | uint8   |                                                                                                                                |
| Uint16               | ○   | ○     | uint16  |                                                                                                                                |
| Uint32               | ○   | ○     | uint32  |                                                                                                                                |
| Uint64               | ○   | ○     | uint64  |                                                                                                                                |
| Float32              | －   | ○     | float32 |                                                                                                                                |
| Float64              | －   | ○     | float64 |                                                                                                                                |
| ShortString          | ○   | ○     | string  | 内部的には[]byteで保存される。0～255バイトに収まる必要がある。バイト長もデータごとに保存される。キーとして使う場合は`strings.Compare`が順序に使用される。 |
| FixedSizeShortString | ○   | ○     | string  | 内部的には[]byteで保存される。テーブル作成時に指定した固定バイトサイズ（1～255バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。キーとして使う場合は`strings.Compare`が順序に使用される。 |
| LongString           | －   | ○     | string  | 内部的には[]byteで保存される。0～65535バイトに収まる必要がある。バイト長もデータごとに保存される。                             |
| FixedSizeLongString  | －   | ○     | string  | 内部的には[]byteで保存される。テーブル作成時に指定した固定バイトサイズ（1～65535バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。 |
| Text                 | －   | ○     | string  | 内部的には[]byteで保存される。0～1073741823バイトに収まる必要がある。バイト長もデータごとに保存される。（データは丸ごとメモリ上にロードされるのでサイズに注意） |
| ShortBytes           | ○   | ○     | []byte  | 0～255バイトに収まる必要がある。バイト長もデータごとに保存される。キーとして使う場合は`bytes.Compare`が順序に使用される。     |
| FixedSizeShortBytes  | ○   | ○     | []byte  | テーブル作成時に指定した固定バイトサイズ（1～255バイト）で保存される。サイズ未満のバイトスライスの場合、指定バイトサイズになるよう`byte(0)`が埋められる。キーとして使う場合は`bytes.Compare`が順序に使用される。 |
| LongBytes            | －   | ○     | []byte  | 0～65535バイトに収まる必要がある。バイト長もデータごとに保存される。                                                           |
| FixedSizeLongBytes   | －   | ○     | []byte  | テーブル作成時に指定した固定バイトサイズ（1～65535バイト）で保存される。サイズ未満のバイトスライスの場合、指定バイトサイズになるよう`byte(0)`が埋められる。 |
| Blob                 | －   | ○     | []byte  | 0～1073741823バイトに収まる必要がある。バイト長もデータごとに保存される。（データは丸ごとメモリ上にロードされるのでサイズに注意） |



##### タグを用いる場合の表記例

カラム名とカラム型をカンマで区切って指定する。カラム型の指定は大文字小文字を区別するので注意。キーとなるフィールドのカラム型には`key@`プリフィクスをつける。カラム型の固定バイト長のサイズは角括弧でカラム型に続けて指定する。

```go
type Foo struct {
	CounterValue unkodb.CounterType `unkodb:"id,key@Counter"`
	Int8value    int8               `unkodb:"i8,Int8"`
	Int16value   int16              `unkodb:"i16,Int16"`
	Int32value   int32              `unkodb:"i32,Int32"`
	Int64value   int64              `unkodb:"i64,Int64"`
	Uint8value   uint8              `unkodb:"u8,Uint8"`
	Uint16value  uint16             `unkodb:"u16,Uint16"`
	Uint32value  uint32             `unkodb:"u32,Uint32"`
	Uint64value  uint64             `unkodb:"u64,Uint64"`
	Float32value float32            `unkodb:"f32,Float32"`
	Float64value float64            `unkodb:"f64,Float64"`
	SSvalue      string             `unkodb:"ss,ShortString"`
	FSSSvalue    string             `unkodb:"fsss,FixedSizeShortString[100]"`
	LSvalue      string             `unkodb:"ls,LongString"`
	FSLSvalue    string             `unkodb:"fsls,FixedSizeLongString[500]"`
	Text         string             `unkodb:"tx,Text"`
	SBvalue      []byte             `unkodb:"sb,ShortBytes"`
	FSSBvalue    []byte             `unkodb:"fssb,FixedSizeShortBytes[20]"`
	LBvalue      []byte             `unkodb:"lb,LongBytes"`
	FSLBvalue    []byte             `unkodb:"fslb,FixedSizeLongBytes[300]"`
	Blob         []byte             `unkodb:"bl,Blob"`
}
```
