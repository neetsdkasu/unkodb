// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"log"
)

var logger = log.New(log.Writer(), "unkodb", log.Flags())

type UnkoDB struct{}

func (db *UnkoDB) CreateTable(tableName string) (*TableCreator, error) {
	// TODO テーブル名の重複チェック
	panic("TODO")
}
