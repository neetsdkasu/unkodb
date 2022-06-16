// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"errors"
)

var (
	KeyAlreadyExists        = errors.New("KeyAlreadyExists")
	ColumnNameAlreadyExists = errors.New("ColumnNameAlreadyExists")
	ColumnNameIsTooLong     = errors.New("ColumnNameIsTooLong")
	InvalidOperation        = errors.New("InvalidOperation")
)

const (
	MaximumColumnNameByteSize = 30
)

type TableCreator struct {
	db            *unkoDB
	name          string
	key           Column
	columns       []Column
	columnNameMap map[string]bool
	created       bool
}

func newTableCreator(db *unkoDB, name string) *TableCreator {
	return &TableCreator{
		db:            db,
		name:          name,
		key:           nil,
		columns:       nil,
		columnNameMap: make(map[string]bool),
		created:       false,
	}
}

func (tc *TableCreator) Create() (*Table, error) {
	if tc.created {
		return nil, InvalidOperation
	}
	table := &Table{
		name:    tc.name,
		key:     tc.key,
		columns: tc.columns,
	}
	// TODO err = tc.db.addTable(table) ?
	tc.db = nil
	tc.name = ""
	tc.key = nil
	tc.columns = nil
	tc.columnNameMap = nil
	tc.created = true
	return table, nil
}

func (tc *TableCreator) has(columnName string) bool {
	_, ok := tc.columnNameMap[columnName]
	return ok
}

func (tc *TableCreator) setKey(column Column) error {
	if tc.created {
		return InvalidOperation
	}
	if tc.key != nil {
		return KeyAlreadyExists
	}
	if len([]byte(column.Name())) > MaximumColumnNameByteSize {
		return ColumnNameIsTooLong
	}
	if tc.has(column.Name()) {
		return ColumnNameAlreadyExists
	}
	tc.key = column
	tc.columnNameMap[column.Name()] = true
	return nil
}

func (tc *TableCreator) addColumn(column Column) error {
	if tc.created {
		return InvalidOperation
	}
	if len([]byte(column.Name())) > MaximumColumnNameByteSize {
		return ColumnNameIsTooLong
	}
	if tc.has(column.Name()) {
		return ColumnNameAlreadyExists
	}
	tc.columns = append(tc.columns, column)
	tc.columnNameMap[column.Name()] = true
	return nil
}

func (tc *TableCreator) Int8Key(newColumnName string) error {
	return tc.setKey(&int8Column{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int8Column(newColumnName string) error {
	return tc.addColumn(&int8Column{
		name: newColumnName,
	})
}
