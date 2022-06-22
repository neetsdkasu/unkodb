// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"
	"io"
)

type UnkoDB struct {
	file       *fileAccessor
	segManager *segmentManager
	tableList  *Table
	tables     []*Table
}

func Create(emptyFile io.ReadWriteSeeker) (db *UnkoDB, err error) {
	defer catchError(&err)
	var file *fileAccessor
	file, err = initializeNewFile(emptyFile)
	if err != nil {
		return
	}
	db = &UnkoDB{
		file:       file,
		segManager: newSegmentManager(file),
		tables:     nil,
	}
	return
}

func Open(dbFile io.ReadWriteSeeker) (db *UnkoDB, err error) {
	defer catchError(&err)
	var file *fileAccessor
	file, err = readFile(dbFile)
	if err != nil {
		return
	}
	// TODO テーブルリスト読み込み？
	db = &UnkoDB{
		file:       file,
		segManager: newSegmentManager(file),
		tableList:  nil,
		tables:     nil,
	}
	err = db.loadTableListTable()
	return
}

func (db *UnkoDB) CreateTable(newTableName string) (creator *TableCreator, err error) {
	defer catchError(&err)
	if db == nil || db.segManager == nil {
		err = UninitializedUnkoDB
		return
	}
	// TODO テーブル名の文字構成ルールチェック（文字列長のチェックくらい？）
	for _, t := range db.tables {
		if t.name == newTableName {
			err = TableNameAlreadyExists
			return
		}
	}
	creator = newTableCreator(db, newTableName)
	return
}

func (db *UnkoDB) newTable(name string, key keyColumn, columns []Column) (*Table, error) {
	// TODO ちゃんと作る
	table := &Table{
		name:         name,
		key:          key,
		columns:      columns,
		rootAccessor: nil,
	}
	table.rootAccessor = table
	return table, nil
}

func (db *UnkoDB) loadTableSpec(tableName string, columnsSpecBuf []byte) (err error) {
	r := newByteDecoder(bytes.NewReader(columnsSpecBuf), fileByteOrder)
	var rootAddress int32
	err = r.Int32(&rootAddress)
	if err != nil {
		return
	}
	var col Column
	col, err = r.ReadColumnSpec()
	if err != nil {
		return
	}
	key, ok := col.(keyColumn)
	if !ok {
		// TODO error
		return
	}
	var colCount uint8
	err = r.Uint8(&colCount)
	if err != nil {
		return
	}
	columns := make([]Column, colCount)
	for i := range columns {
		col, err = r.ReadColumnSpec()
		if err != nil {
			return err
		}
		columns[i] = col
	}
	table := &Table{
		db:             db,
		name:           tableName,
		key:            key,
		columns:        columns,
		rootAddress:    int(rootAddress),
		columnsSpecBuf: columnsSpecBuf,
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

func (db *UnkoDB) loadTableListTable() error {
	db.tableList = &Table{
		db:           db,
		name:         tableListTableName,
		key:          &shortStringColumn{name: tableListKeyName},
		columns:      []Column{&longBytesColumn{name: tableListColumnName}},
		rootAccessor: db,
	}
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
