// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

type TableCreator struct {
	db            *UnkoDB
	name          string
	key           keyColumn
	columns       []Column
	columnNameMap map[string]bool
	created       bool
}

func newTableCreator(db *UnkoDB, name string) *TableCreator {
	return &TableCreator{
		db:            db,
		name:          name,
		key:           nil,
		columns:       nil,
		columnNameMap: make(map[string]bool),
		created:       false,
	}
}

func (tc *TableCreator) Create() (table *Table, err error) {
	defer catchError(&err)
	if tc.created {
		err = InvalidOperation
		return
	}
	if tc.key == nil {
		err = NeedToSetAKey
		return
	}
	table, err = tc.db.newTable(tc.name, tc.key, tc.columns)
	if err != nil {
		return
	}
	tc.db = nil
	tc.name = ""
	tc.key = nil
	tc.columns = nil
	tc.columnNameMap = nil
	tc.created = true
	return
}

func (tc *TableCreator) has(columnName string) bool {
	_, ok := tc.columnNameMap[columnName]
	return ok
}

func (tc *TableCreator) setKey(column keyColumn) error {
	if tc.created {
		return InvalidOperation
	}
	if tc.key != nil {
		return KeyAlreadyExists
	}
	// TODO カラム名の文字構成チェック（長さゼロの文字列ダメとか？）
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
	// TODO カラム名の文字構成チェック（長さゼロの文字列ダメとか？）
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
	return tc.setKey(&intColumn[int8]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int8Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int8]{
		name: newColumnName,
	})
}

func (tc *TableCreator) ShortStringKey(newColumnName string) error {
	return tc.setKey(&shortStringColumn{
		name: newColumnName,
	})
}

func (tc *TableCreator) ShortStringColumn(newColumnName string) error {
	return tc.addColumn(&shortStringColumn{
		name: newColumnName,
	})
}
