// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"log"
)

var logger = log.New(log.Writer(), "unkodb", log.Flags())

type UnkoDB interface {
	CreateTable(tableName string) (*TableCreator, error)
}

type unkoDB struct{}

func (db *unkoDB) CreateTable(tableName string) (*TableCreator, error) {
	// TODO テーブル名の重複チェック
	return newTableCreator(db, tableName), nil
}
