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
	tables []Table
}

var (
	TableNameAlreadyExists = errors.New("TableNameAlreadyExists")
)

func Create(emptyFile io.ReadWriteSeeker) (*UnkoDB, error) {
	panic("TODO")
}

func Open(dbFile io.ReadWriteSeeker) (*UnkoDB, error) {
	panic("TODO")
}

func (db *UnkoDB) CreateTable(newTableName string) (*TableCreator, error) {
	// TODO テーブル名の文字構成ルールチェック（文字列長のチェックくらい？）
	for _, t := range db.tables {
		if t.name == newTableName {
			return nil, TableNameAlreadyExists
		}
	}
	return newTableCreator(db, newTableName), nil
}
