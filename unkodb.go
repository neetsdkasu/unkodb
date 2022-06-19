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

func Create(emptyFile io.ReadWriteSeeker) (*UnkoDB, error) {
	file, err := initializeNewFile(emptyFile)
	if err != nil {
		return nil, err
	}
	db := &UnkoDB{
		segManager: newSegmentManager(file),
		tables:     nil,
	}
	return db, nil
}

func Open(dbFile io.ReadWriteSeeker) (*UnkoDB, error) {
	file, err := readFile(dbFile)
	if err != nil {
		return nil, err
	}
	// TODO テーブルリスト読み込み？
	db := &UnkoDB{
		segManager: newSegmentManager(file),
		tables:     nil,
	}
	return db, nil
}

func (db *UnkoDB) CreateTable(newTableName string) (*TableCreator, error) {
	if db == nil || db.segManager == nil {
		return nil, UninitializedUnkoDB
	}
	// TODO テーブル名の文字構成ルールチェック（文字列長のチェックくらい？）
	for _, t := range db.tables {
		if t.name == newTableName {
			return nil, TableNameAlreadyExists
		}
	}
	return newTableCreator(db, newTableName), nil
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
