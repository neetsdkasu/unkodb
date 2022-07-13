// unkodb
// author: Leonardone @ NEETSDKASU

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
//
// 		file, _ := os.Create("my_data.unkodb")
// 		db, _ := unkodb.Create(file)
//
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
//
// 		file, _ := os.OpenFile("my_data.unkodb", os.O_RDWR, 0755)
// 		db, _ := unkodb.Open(file)
//
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
// 		table := db.Table("my_book_table")
// 		if table == nil {
// 			// my_book_table is not existed in db
// 		} else {
// 			// table is my_book_table
// 		}
//
func (db *UnkoDB) Table(name string) *Table {
	for _, table := range db.tables {
		if table.Name() == name {
			return table
		}
	}
	return nil
}

// 指定の名前のテーブルを削除する。
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
		err = NotFoundTable
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
//
// 		tc, _ := db.CreateTable("my_book_table")
// 		tc.CounterKey("id")
// 		tc.ShortStringColumn("title")
// 		tc.ShortStringColumn("author")
// 		tc.ShortStringColumn("genre")
// 		table, _ := tc.Create()
//
func (db *UnkoDB) CreateTable(newTableName string) (creator *TableCreator, err error) {
	if !debugMode {
		defer catchError(&err)
	}
	if db == nil || db.segManager == nil {
		err = UninitializedUnkoDB
		return
	}
	// TODO テーブル名の文字構成ルールチェック（文字列長のチェックくらい？）
	if len([]byte(newTableName)) > MaximumTableNameByteSize {
		err = TableNameIsTooLong
		return
	}
	for _, t := range db.tables {
		if t.name == newTableName {
			err = TableNameAlreadyExists
			return
		}
	}
	creator = newTableCreator(db, newTableName)
	return
}

// 指定した名前の新しいテーブルをunkodbタグの情報を元に構築する。
// テーブル名は他のテーブル名と重複はできない。
//
// 		type Book struct {
// 			Id     unkodb.CounterType `unkodb:"id,key@Counter"`
// 			Title  string             `unkodb:"title,ShortString"`
// 			Author string             `unkodb:"author,ShortString"`
// 			Genre  string             `unkodb:"genre,ShortString"`
// 		}
// 		table, _ := db.CreateTableByTaggedStruct("my_book_table", (*Book)(nil))
//
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
			err = WrongFileFormat{"invalid dataSeparation"}
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
			err = WrongFileFormat{fmt.Sprintf("invalid key in %s", tableName)}
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
