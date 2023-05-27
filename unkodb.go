// unkodb
// author: Leonardone @ NEETSDKASU

// # unkodb
//
// データを単一ファイルに読み書きする感じ？
//
// ＤＢではないです。
//
// - ファイルサイズは2GB以下までしか扱えない。
//
// - ファイルに対しては直接の操作ではなくインターフェース（`io.ReadWriteSeeker`）越しの読み書きしか行わない（共有ロックや`Flush`や`Close`などの処理等は呼び出し側のほうで行う必要がある）。
//
// - データのサイズの変わる更新や削除を行うと使用できないゴミ領域が発生するが対処はしてない。
//
// - テーブルの名前やカラムを変える仕組みは無い。
//
// - 無駄なIO処理やメモリ確保が多いため大量のデータの取り扱いや頻繁なアクセスには向いてない。
//
// - トランザクションのような仕組みは無い。
//
// - スレッドセーフではない。
//
// - フェイルセーフではない。
//
// - ファイルフォーマットを確認しないため不正なファイル読み込みでパニックするかも。
//
// - テーブル名とカラム名は1バイト以上255バイト以下で指定する必要がある（Goのstringを[]byteにキャストした際のサイズ）。
//
// - テーブル名とカラム名に使える文字は今のところ制限は設けていない。
//
// - カラム数はテーブルごとに100個まで。
//
// - 内部的にはAVL木で管理されている（AVL木の実装が正しければよいが･･･）。
// /
// - 各テーブルにキーを１つ指定する。
//
// - データの検索はキーでのみ行える（キーの重複は許されてない）。
//
// - デバッグ不十分なのでバグだらけなのでバグでデータが破壊される可能性が高いです（死）。
//
//	package example
//	import (
//		"fmt"
//		"log"
//		"os"
//		"github.com/neetsdkasu/unkodb"
//	)
//	func ExampleUnkoDB() {
//		// Exampleのテスト用のファイルなのでテスト実行後は削除する･･･
//		const FileExists = false
//		file, err := os.CreateTemp("", "example.unkodb")
//		if err != nil {
//			log.Fatal(err)
//		}
//		defer os.Remove(file.Name())
//		var (
//			db    *unkodb.UnkoDB
//			table *unkodb.Table
//		)
//		if FileExists {
//			// file, err := os.OpenFile("example.unkodb", os.O_RDWR, 0755)
//			// defer file.Close()
//			db, err = unkodb.Open(file)
//			if err != nil {
//				log.Fatal(err)
//			}
//			table = db.Table("food_table")
//			if table == nil {
//				log.Fatal("not found food_table")
//			}
//		} else {
//			// file, err := os.Create("example.unkodb")
//			// defer file.Close()
//			db, err = unkodb.Create(file)
//			if err != nil {
//				log.Fatal(err)
//			}
//			tc, err := db.CreateTable("food_table")
//			if err != nil {
//				log.Fatal(err)
//			}
//			tc.CounterKey("id")
//			tc.ShortStringColumn("name")
//			tc.Int64Column("price")
//			table, err = tc.Create()
//			if err != nil {
//				log.Fatal(err)
//			}
//			list := []map[string]any{
//				map[string]any{
//					"id":    unkodb.CounterType(0),
//					"name":  "クリームパン",
//					"price": int64(234),
//				},
//				map[string]any{
//					"id":    unkodb.CounterType(0),
//					"name":  "あんぱん",
//					"price": int64(123),
//				},
//				map[string]any{
//					"id":    unkodb.CounterType(0),
//					"name":  "カレーパン",
//					"price": int64(345),
//				},
//				map[string]any{
//					"id":    unkodb.CounterType(0),
//					"name":  "ジャムパン",
//					"price": int64(222),
//				},
//				map[string]any{
//					"id":    unkodb.CounterType(0),
//					"name":  "食パン",
//					"price": int64(333),
//				},
//			}
//			// Insert
//			for _, item := range list {
//				_, err = table.Insert(item)
//				if err != nil {
//					log.Fatal(err)
//				}
//			}
//			// Delete id=3 カレーパン
//			err = table.Delete(unkodb.CounterType(3))
//			if err != nil {
//				log.Fatal(err)
//			}
//			// Replace id=4 ジャムパン
//			replace := map[string]any{
//				"id":    unkodb.CounterType(4),
//				"name":  "イチゴジャムパン",
//				"price": int64(987),
//			}
//			_, err = table.Replace(replace)
//			if err != nil {
//				log.Fatal(err)
//			}
//		}
//		// Find id=2 あんぱん
//		r, err := table.Find(unkodb.CounterType(2))
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("[FIND] ID: %d, NAME: %s, PRICE: %d\n", r.Key(), r.Column("name"), r.Column("price"))
//		// Iteration データを順番に辿る
//		err = table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
//			fmt.Printf("[ITER] id: %d, name: %s, price: %d\n", r.Column("id"), r.Column("name"), r.Column("price"))
//			return
//		})
//		if err != nil {
//			log.Fatal(err)
//		}
//		// Output:
//		// [FIND] ID: 2, NAME: あんぱん, PRICE: 123
//		// [ITER] id: 1, name: クリームパン, price: 234
//		// [ITER] id: 2, name: あんぱん, price: 123
//		// [ITER] id: 4, name: イチゴジャムパン, price: 987
//		// [ITER] id: 5, name: 食パン, price: 333
//	}
package unkodb

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/neetsdkasu/avltree/stringkey"
)

type UnkoDB struct {
	file       *fileAccessor
	segManager *segmentManager
	tableList  *Table
	tables     []*Table
}

// 空の新しいファイルにUnkoDBを構築する。
// IOエラーなどがある場合に戻り値のエラーにはnil以外が返る。(たいていプログラムの実行にとって致命的エラー)。
//
//	file, _ := os.Create("my_data.unkodb")
//	db, _ := unkodb.Create(file)
func Create(emptyFile io.ReadWriteSeeker) (db *UnkoDB, err error) {
	if !debugMode {
		defer catchError(&err)
	}
	var file *fileAccessor
	file, err = initializeNewFile(emptyFile)
	if err != nil {
		return
	}
	db = &UnkoDB{
		file:       file,
		segManager: newSegmentManager(file),
		tableList:  nil,
		tables:     nil,
	}
	err = db.initTableListTable()
	if err != nil {
		db = nil
	}
	return
}

// UnkoDB構築済みのファイルからUnkoDBを開く。
// IOエラーや不正なファイルのときのエラーなどがある場合に戻り値のエラーにはnil以外が返る。(たいていプログラムの実行にとって致命的エラー)。
//
//	file, _ := os.OpenFile("my_data.unkodb", os.O_RDWR, 0755)
//	db, _ := unkodb.Open(file)
func Open(dbFile io.ReadWriteSeeker) (db *UnkoDB, err error) {
	if !debugMode {
		defer catchError(&err)
	}
	var file *fileAccessor
	file, err = readFile(dbFile)
	if err != nil {
		return
	}
	db = &UnkoDB{
		file:       file,
		segManager: newSegmentManager(file),
		tableList:  nil,
		tables:     nil,
	}
	err = db.initTableListTable()
	if err != nil {
		db = nil
	}
	return
}

// テーブルのリストを取得する。
func (db *UnkoDB) Tables() []*Table {
	list := make([]*Table, len(db.tables))
	copy(list, db.tables)
	return list
}

// 指定の名前のテーブルを取得する。
// 指定した名前のテーブルが存在しない場合はnilを返す。
//
//	table := db.Table("my_book_table")
//	if table == nil {
//		// my_book_table is not existed in db
//	} else {
//		// table is my_book_table
//	}
func (db *UnkoDB) Table(name string) *Table {
	for _, table := range db.tables {
		if table.Name() == name {
			return table
		}
	}
	return nil
}

// 指定の名前のテーブルを削除する。
// テーブル名が存在しない場合はNotFoundTableのエラーが返る。
// それ以外のエラー（IOエラーなど）がある場合にも戻り値エラーはnil以外が返る。(たいていプログラムの実行にとって致命的エラー)。
func (db *UnkoDB) DeleteTable(name string) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	var index = 0
	var table *Table = nil
	for i, t := range db.tables {
		if t.Name() == name {
			index = i
			table = t
			break
		}
	}
	if table == nil {
		err = ErrNotFoundTable
		return
	}
	err = table.deleteAll()
	if err != nil {
		return
	}
	err = db.tableList.Delete(name)
	if err != nil {
		return
	}
	list := []*Table{}
	for i, t := range db.tables {
		if i != index {
			list = append(list, t)
		}
	}
	db.tables = list
	return
}

// 指定した名前の新しいテーブルを作成するためのTableCreaetorを返す。
// テーブル名は他のテーブル名と重複はできない。
// TableCreatorのCreateメソッドを呼び出すまではdbにテーブルは構築されない。
// テーブル名に不正がある場合には対応したエラーが返る。
//
//	tc, _ := db.CreateTable("my_book_table")
//	tc.CounterKey("id")
//	tc.ShortStringColumn("title")
//	tc.ShortStringColumn("author")
//	tc.ShortStringColumn("genre")
//	table, _ := tc.Create()
func (db *UnkoDB) CreateTable(newTableName string) (creator *TableCreator, err error) {
	if !debugMode {
		defer catchError(&err)
	}
	// TODO テーブル名の文字構成ルールチェック（文字列長のチェックくらい？）
	if len([]byte(newTableName)) > MaximumTableNameByteSize {
		err = ErrTableNameIsTooLong
		return
	}
	for _, t := range db.tables {
		if t.name == newTableName {
			err = ErrTableNameAlreadyExists
			return
		}
	}
	creator = newTableCreator(db, newTableName)
	return
}

// 指定した名前の新しいテーブルをunkodbタグの情報を元に構築する。
// テーブル名は他のテーブル名と重複はできない。
// テーブル名やカラム名やキーやカラムの設定の仕方に不正がある場合には対応したエラーが返る。
// それ以外のエラー（IOエラーなど）がある場合にも戻り値エラーはnil以外が返る。(たいていプログラムの実行にとって致命的エラー)。
//
//	type Book struct {
//		Id     unkodb.CounterType `unkodb:"id,key@Counter"`
//		Title  string             `unkodb:"title,ShortString"`
//		Author string             `unkodb:"author,ShortString"`
//		Genre  string             `unkodb:"genre,ShortString"`
//	}
//	table, _ := db.CreateTableByTaggedStruct("my_book_table", (*Book)(nil))
func (db *UnkoDB) CreateTableByTaggedStruct(newTableName string, taggedStruct any) (table *Table, err error) {
	if !debugMode {
		defer catchError(&err)
	}
	var creator *TableCreator
	creator, err = db.CreateTable(newTableName)
	if err != nil {
		return
	}
	err = createTableByTaggedStruct(creator, taggedStruct)
	if err != nil {
		return
	}
	table, err = creator.Create()
	return
}

// キーとカラムの構造が指定のテーブルと同じ構造の新しいテーブルを作成する。
// テーブル名に不正がある場合には対応したエラーが返る。
// それ以外のエラー（IOエラーなど）がある場合にも戻り値エラーはnil以外が返る。(たいていプログラムの実行にとって致命的エラー)。
//
//	tc, _ := myDB.CreateTable("my_book_table")
//	tc.CounterKey("id")
//	tc.ShortStringColumn("title")
//	tc.ShortStringColumn("author")
//	tc.ShortStringColumn("genre")
//	myTable, _ := tc.Create()
//	mySecret, _ := myDB.CreateTableByOtherTable("my_secret_book_table", myTable)
//	yourTable, _ := yourDB.CreateTableByOtherTable("your_book_table", myTable)
//	yourSecret, _ := yourDB.CreateTableByOtherTable("your_secret_book_table", myTable)
func (db *UnkoDB) CreateTableByOtherTable(newTableName string, other *Table) (table *Table, err error) {
	if !debugMode {
		defer catchError(&err)
	}
	_, err = db.CreateTable(newTableName)
	if err != nil {
		return
	}
	table, err = db.newTable(newTableName, other.key, other.columns, other.dataSeparation)
	return
}

func (db *UnkoDB) newTable(name string, key keyColumn, columns []Column, dataSeparation dataSeparationState) (*Table, error) {
	table := &Table{
		db:             db,
		name:           name,
		key:            key,
		columns:        columns,
		nodeCount:      0,
		counter:        0,
		rootAddress:    nullAddress,
		rootAccessor:   nil,
		columnsSpecBuf: nil,
		dataSeparation: dataSeparation,
	}
	table.rootAccessor = table
	var b bytes.Buffer
	w := newByteEncoder(&b, fileByteOrder)
	// tableSpecHeader
	{
		err := w.Int32(int32(table.rootAddress))
		if err != nil {
			return nil, err
		}
		err = w.Int32(int32(table.nodeCount))
		if err != nil {
			return nil, err
		}
		err = w.Uint32(uint32(table.counter))
		if err != nil {
			return nil, err
		}
		err = w.Uint8(uint8(table.dataSeparation))
		if err != nil {
			return nil, err
		}
	}
	// tableSpecKeyAndColumns
	{
		err := w.WriteColumnSpec(table.key)
		if err != nil {
			return nil, err
		}
		err = w.Uint8(uint8(len(table.columns)))
		if err != nil {
			return nil, err
		}
		for _, col := range table.columns {
			err = w.WriteColumnSpec(col)
			if err != nil {
				return nil, err
			}
		}
	}
	table.columnsSpecBuf = b.Bytes()
	data := make(map[string]any)
	data[tableListKeyName] = table.name
	data[tableListColumnName] = table.columnsSpecBuf
	_, err := db.tableList.Insert(data)
	if err != nil {
		return nil, err
	}
	db.tables = append(db.tables, table)
	sort.Slice(db.tables, func(i, j int) bool {
		key1 := stringkey.StringKey(db.tables[i].name)
		key2 := stringkey.StringKey(db.tables[j].name)
		return key1.CompareTo(key2) < 0
	})
	return table, nil
}

func (db *UnkoDB) loadTableSpec(tableName string, columnsSpecBuf []byte) (err error) {
	r := newByteDecoder(bytes.NewReader(columnsSpecBuf), fileByteOrder)
	// tableSpecHeader
	var (
		rootAddress    int32
		nodeCount      int32
		counter        uint32
		dataSeparation uint8
	)
	{
		err = r.Int32(&rootAddress)
		if err != nil {
			return
		}
		err = r.Int32(&nodeCount)
		if err != nil {
			return
		}
		err = r.Uint32(&counter)
		if err != nil {
			return
		}
		err = r.Uint8(&dataSeparation)
		if err != nil {
			return
		}
		if !dataSeparationState(dataSeparation).IsValid() {
			err = ErrWrongFileFormat{"invalid dataSeparation"}
			return
		}
	}
	// tableSpecKeyAndColumns
	var (
		key     keyColumn
		columns []Column
	)
	{
		var col Column
		col, err = r.ReadColumnSpec()
		if err != nil {
			return
		}
		var ok bool
		key, ok = col.(keyColumn)
		if !ok {
			// TODO ちゃんとしたエラー作る
			err = ErrWrongFileFormat{fmt.Sprintf("invalid key in %s", tableName)}
			return
		}
		var colCount uint8
		err = r.Uint8(&colCount)
		if err != nil {
			return
		}
		columns = make([]Column, colCount)
		for i := range columns {
			col, err = r.ReadColumnSpec()
			if err != nil {
				return err
			}
			columns[i] = col
		}
	}
	table := &Table{
		db:             db,
		name:           tableName,
		key:            key,
		columns:        columns,
		nodeCount:      int(nodeCount),
		counter:        uint(counter),
		rootAddress:    int(rootAddress),
		columnsSpecBuf: columnsSpecBuf,
		dataSeparation: dataSeparationState(dataSeparation),
	}
	table.rootAccessor = table
	db.tables = append(db.tables, table)
	return
}

func (db *UnkoDB) getRootAddress() (addr int, err error) {
	addr = db.file.TableListRootAddress()
	return
}
func (db *UnkoDB) setRootAddress(addr int) (err error) {
	err = db.file.UpdateTableListRootAddress(addr)
	return
}

func (db *UnkoDB) initTableListTable() error {
	db.tableList = &Table{
		db:             db,
		name:           tableListTableName,
		key:            &shortStringColumn{name: tableListKeyName},
		columns:        []Column{&longBytesColumn{name: tableListColumnName}},
		rootAccessor:   db,
		dataSeparation: dataSeparationEnabled,
	}
	// TODO データが壊れててテーブル名が重複してたりカラム情報が壊れてたりの対処は？
	err := db.tableList.IterateAll(func(rec *Record) (_ bool) {
		tableName := rec.Key().(string)
		columnsSpecBuf := rec.Column(tableListColumnName).([]byte)
		err := db.loadTableSpec(tableName, columnsSpecBuf)
		if err != nil {
			panic(err)
		}
		return
	})
	return err
}
