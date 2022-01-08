package unkodb

import (
	"github.com/neetsdkasu/unkodb/columntype"
	"github.com/neetsdkasu/unkodb/keytype"
)

type ColumnSpec interface {
	Name() string
	Type() columntype.ColumnType
}

type columnSpec struct {
	name       string
	columnType columntype.ColumnType
}

type Table interface {
	Name() string
	Columns() []ColumnSpec
	spec() *tableSpec
}

type tableSpec struct {
	address     int
	name        string
	keyType     keytype.KeyType
	keyName     string
	rootEntry   int // ルートのインデックス
	entryCount  int // 要素数
	nextEntryId int // CountIdのときのみ使用
	columns     []*columnSpec
}
