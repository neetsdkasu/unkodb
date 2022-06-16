// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"errors"
)

var (
	KeyAlreadyExists        = errors.New("KeyAlreadyExists")
	ColumnNameAlreadyExists = errors.New("ColumnNameAlreadyExists")
)

type TableCreator struct {
	name          string
	key           Column
	columns       []Column
	columnNameMap map[string]bool
}

func (tc *TableCreator) Has(columnName string) bool {
	_, ok := tc.columnNameMap[columnName]
	return ok
}

func (tc *TableCreator) setKey(column Column) error {
	if tc.key != nil {
		return KeyAlreadyExists
	}
	if tc.Has(column.Name()) {
		return ColumnNameAlreadyExists
	}
	tc.key = column
	tc.columnNameMap[column.Name()] = true
	return nil
}

func (tc *TableCreator) addColumn(column Column) error {
	if tc.Has(column.Name()) {
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
