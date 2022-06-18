// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"errors"
	"io"
	"log"
)

var logger = log.New(log.Writer(), "unkodb", log.Flags())

type UnkoDB struct {
	manager *segmentManager
	tables  []Table
}

var (
	TableNameAlreadyExists = errors.New("TableNameAlreadyExists")
	UninitializedUnkoDB    = errors.New("UninitializedUnkoDB")
)

func Create(emptyFile io.ReadWriteSeeker) (*UnkoDB, error) {
	file, err := initializeNewFile(emptyFile)
	if err != nil {
		return nil, err
	}
	db := &UnkoDB{
		manager: newSegmentManager(file),
		tables:  nil,
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
		manager: newSegmentManager(file),
		tables:  nil,
	}
	return db, nil
}

func (db *UnkoDB) CreateTable(newTableName string) (*TableCreator, error) {
	if db == nil || db.manager == nil {
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
