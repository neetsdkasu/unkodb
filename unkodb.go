// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"io"
)

type UnkoDB struct {
	segManager *segmentManager
	tables     []Table
}

func Create(emptyFile io.ReadWriteSeeker) (db *UnkoDB, err error) {
	defer catchError(&err)
	var file *fileAccessor
	file, err = initializeNewFile(emptyFile)
	if err != nil {
		return
	}
	db = &UnkoDB{
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
		segManager: newSegmentManager(file),
		tables:     nil,
	}
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

func (db *UnkoDB) newTable(name string, key Column, columns []Column) (*Table, error) {
	// TODO ちゃんと作る
	table := &Table{
		name:    name,
		key:     key,
		columns: columns,
	}
	return table, nil
}
