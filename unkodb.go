// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
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
		name:    name,
		key:     key,
		columns: columns,
	}
	return table, nil
}

func (db *UnkoDB) loadTableSpec(tableName string, colomnsSpecBuf []byte) {
	// TODO
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
		name:         "table_list",
		key:          &shortStringColumn{name: "table_name"},
		columns:      []Column{&longBytesColumn{name: "colomns_spec_buf"}},
		rootAccessor: db,
	}
	err := db.tableList.IterateAll(func(rec *Record) (_ bool) {
		tableName := rec.Key().(string)
		colomnsSpecBuf := rec.Column("colomns_spec_buf").([]byte)
		db.loadTableSpec(tableName, colomnsSpecBuf)
		return
	})
	return err
}
