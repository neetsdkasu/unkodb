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
	if !debugMode {
		defer catchError(&err)
	}
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
	// TODO カラム名の文字構成チェックいる？？
	if len([]byte(column.Name())) == 0 {
		return NeedColumnName
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
	if len(tc.columns) >= MaximumColumnCountWithoutKey {
		return ColumnCountIsFull
	}
	// TODO カラム名の文字構成チェックいる？？
	if len([]byte(column.Name())) == 0 {
		return NeedColumnName
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
	return tc.setKey(&intColumn[int8]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int8Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int8]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Uint8Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint8]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Uint8Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint8]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int16Key(newColumnName string) error {
	return tc.setKey(&intColumn[int16]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int16Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int16]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Uint16Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint16]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Uint16Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint16]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int32Key(newColumnName string) error {
	return tc.setKey(&intColumn[int32]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int32Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int32]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Uint32Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint32]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Uint32Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint32]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int64Key(newColumnName string) error {
	return tc.setKey(&intColumn[int64]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Int64Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int64]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Uint64Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint64]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Uint64Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint64]{
		name: newColumnName,
	})
}

func (tc *TableCreator) CounterKey(newColumnName string) error {
	return tc.setKey(&counterColumn{
		name: newColumnName,
	})
}

func (tc *TableCreator) Float32Column(newColumnName string) error {
	return tc.addColumn(&floatColumn[float32]{
		name: newColumnName,
	})
}

func (tc *TableCreator) Float64Column(newColumnName string) error {
	return tc.addColumn(&floatColumn[float64]{
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

func (tc *TableCreator) FixedSizeShortStringKey(newColumnName string, size uint8) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.setKey(&fixedSizeShortStringColumn{
		name: newColumnName,
		size: size,
	})
}

func (tc *TableCreator) FixedSizeShortStringColumn(newColumnName string, size uint8) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeShortStringColumn{
		name: newColumnName,
		size: size,
	})
}

func (tc *TableCreator) LongStringColumn(newColumnName string) error {
	return tc.addColumn(&longStringColumn{
		name: newColumnName,
	})
}

func (tc *TableCreator) FixedSizeLongStringColumn(newColumnName string, size uint16) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeLongStringColumn{
		name: newColumnName,
		size: size,
	})
}

func (tc *TableCreator) TextColumn(newColumnName string) error {
	return tc.addColumn(&textColumn{
		name: newColumnName,
	})
}

func (tc *TableCreator) ShortBytesKey(newColumnName string) error {
	return tc.setKey(&shortBytesColumn{
		name: newColumnName,
	})
}

func (tc *TableCreator) ShortBytesColumn(newColumnName string) error {
	return tc.addColumn(&shortBytesColumn{
		name: newColumnName,
	})
}

func (tc *TableCreator) FixedSizeShortBytesKey(newColumnName string, size uint8) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.setKey(&fixedSizeShortBytesColumn{
		name: newColumnName,
		size: size,
	})
}

func (tc *TableCreator) FixedSizeShortBytesColumn(newColumnName string, size uint8) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeShortBytesColumn{
		name: newColumnName,
		size: size,
	})
}

func (tc *TableCreator) LongBytesColumn(newColumnName string) error {
	return tc.addColumn(&longBytesColumn{
		name: newColumnName,
	})
}

func (tc *TableCreator) FixedSizeLongBytesColumn(newColumnName string, size uint16) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeLongBytesColumn{
		name: newColumnName,
		size: size,
	})
}

func (tc *TableCreator) BlobColumn(newColumnName string) error {
	return tc.addColumn(&blobColumn{
		name: newColumnName,
	})
}
