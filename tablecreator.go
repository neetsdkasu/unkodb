// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

// 新しいテーブルの作成に使用される。
//
// 		var tc *TableCreator
// 		tc, _ = db.CreateTable("my_book_table")
// 		tc.CounterKey("id")
// 		tc.ShortStringColumn("title")
// 		tc.ShortStringColumn("author")
// 		tc.ShortStringColumn("genre")
// 		table, _ := tc.Create()
//
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

// 設定したキーとカラムを持つテーブルを作成する。
//
// 		var tc *TableCreator
// 		tc, _ = db.CreateTable("my_book_table")
// 		tc.CounterKey("id")
// 		tc.ShortStringColumn("title")
// 		tc.ShortStringColumn("author")
// 		tc.ShortStringColumn("genre")
// 		table, _ := tc.Create()
//
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

// Int8のキーを設定する。
func (tc *TableCreator) Int8Key(newColumnName string) error {
	return tc.setKey(&intColumn[int8]{
		name: newColumnName,
	})
}

// Int8のカラムを追加する。
func (tc *TableCreator) Int8Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int8]{
		name: newColumnName,
	})
}

// Uint8のキーを設定する。
func (tc *TableCreator) Uint8Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint8]{
		name: newColumnName,
	})
}

// Uint8のカラムを追加する。
func (tc *TableCreator) Uint8Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint8]{
		name: newColumnName,
	})
}

// Int16のキーを設定する。
func (tc *TableCreator) Int16Key(newColumnName string) error {
	return tc.setKey(&intColumn[int16]{
		name: newColumnName,
	})
}

// Int16のカラムを追加する。
func (tc *TableCreator) Int16Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int16]{
		name: newColumnName,
	})
}

// Uint16のキーを設定する。
func (tc *TableCreator) Uint16Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint16]{
		name: newColumnName,
	})
}

// Uint16のカラムを追加する。
func (tc *TableCreator) Uint16Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint16]{
		name: newColumnName,
	})
}

// Int32のキーを設定する。
func (tc *TableCreator) Int32Key(newColumnName string) error {
	return tc.setKey(&intColumn[int32]{
		name: newColumnName,
	})
}

// Int32のカラムを追加する。
func (tc *TableCreator) Int32Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int32]{
		name: newColumnName,
	})
}

// Uint32のキーを設定する。
func (tc *TableCreator) Uint32Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint32]{
		name: newColumnName,
	})
}

// Uint32のカラムを追加する。
func (tc *TableCreator) Uint32Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint32]{
		name: newColumnName,
	})
}

// Int64のキーを設定する。
func (tc *TableCreator) Int64Key(newColumnName string) error {
	return tc.setKey(&intColumn[int64]{
		name: newColumnName,
	})
}

// Int64のカラムを追加する。
func (tc *TableCreator) Int64Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int64]{
		name: newColumnName,
	})
}

// Uint64のキーを設定する。
func (tc *TableCreator) Uint64Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint64]{
		name: newColumnName,
	})
}

// Uint64のカラムを追加する。
func (tc *TableCreator) Uint64Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint64]{
		name: newColumnName,
	})
}

// Counterのキーを設定する。
// 値はGoのuint32の型として扱われる。
// データが挿入時にファイルにCounterの値が書き込まれる。データの挿入ごとに1ずつ値が増えていく（最初は1から始まる）。unkodb.CounterTypeはuint32のエイリアス。
func (tc *TableCreator) CounterKey(newColumnName string) error {
	return tc.setKey(&counterColumn{
		name: newColumnName,
	})
}

// Float32のカラムを追加する。
func (tc *TableCreator) Float32Column(newColumnName string) error {
	return tc.addColumn(&floatColumn[float32]{
		name: newColumnName,
	})
}

// Float64のカラムを追加する。
func (tc *TableCreator) Float64Column(newColumnName string) error {
	return tc.addColumn(&floatColumn[float64]{
		name: newColumnName,
	})
}

// ShortStringのキーを設定する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。0～255バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。キーとして使う場合は`strings.Compare`が順序に使用される。
func (tc *TableCreator) ShortStringKey(newColumnName string) error {
	return tc.setKey(&shortStringColumn{
		name: newColumnName,
	})
}

// ShortStringのカラムを追加する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。0～255バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
func (tc *TableCreator) ShortStringColumn(newColumnName string) error {
	return tc.addColumn(&shortStringColumn{
		name: newColumnName,
	})
}

// FixedSizeShortStringのキーを設定する。
// sizeは1～255の範囲の中から指定する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。テーブル作成時に指定した固定バイトサイズ（1～255バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。キーとして使う場合は`strings.Compare`が順序に使用される。
func (tc *TableCreator) FixedSizeShortStringKey(newColumnName string, size uint8) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.setKey(&fixedSizeShortStringColumn{
		name: newColumnName,
		size: size,
	})
}

// FixedSizeShortStringのカラムを追加する。
// sizeは1～255の範囲の中から指定する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。テーブル作成時に指定した固定バイトサイズ（1～255バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。
func (tc *TableCreator) FixedSizeShortStringColumn(newColumnName string, size uint8) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeShortStringColumn{
		name: newColumnName,
		size: size,
	})
}

// LongStringのカラムを追加する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。0～65535バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
func (tc *TableCreator) LongStringColumn(newColumnName string) error {
	return tc.addColumn(&longStringColumn{
		name: newColumnName,
	})
}

// FixedSizeLongStringのカラムを追加する。
// sizeは1～65535の範囲の中から指定する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。テーブル作成時に指定した固定バイトサイズ（1～65535バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。
func (tc *TableCreator) FixedSizeLongStringColumn(newColumnName string, size uint16) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeLongStringColumn{
		name: newColumnName,
		size: size,
	})
}

// Textのカラムを追加する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。0～1073741823バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
func (tc *TableCreator) TextColumn(newColumnName string) error {
	return tc.addColumn(&textColumn{
		name: newColumnName,
	})
}

// ShortBytesのキーを設定する。
// 値はGoの[]byteとして扱われる。
// 0～255バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。キーとして使う場合はbytes.Compareが順序に使用される。
func (tc *TableCreator) ShortBytesKey(newColumnName string) error {
	return tc.setKey(&shortBytesColumn{
		name: newColumnName,
	})
}

// ShortBytesのカラムを追加する。
// 値はGoの[]byteとして扱われる。
// 0～255バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
func (tc *TableCreator) ShortBytesColumn(newColumnName string) error {
	return tc.addColumn(&shortBytesColumn{
		name: newColumnName,
	})
}

// FixedSizeShortBytesのキーを設定する。
// sizeは1～255の範囲の中から指定する。
// 値はGoの[]byteとして扱われる。
// テーブル作成時に指定した固定バイトサイズ（1～255バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。キーとして使う場合は`bytes.Compare`が順序に使用される。
func (tc *TableCreator) FixedSizeShortBytesKey(newColumnName string, size uint8) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.setKey(&fixedSizeShortBytesColumn{
		name: newColumnName,
		size: size,
	})
}

// FixedSizeShortBytesのカラムを追加する。
// sizeは1～255の範囲の中から指定する。
// 値はGoの[]byteとして扱われる。
// テーブル作成時に指定した固定バイトサイズ（1～255バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう値0が埋められる。
func (tc *TableCreator) FixedSizeShortBytesColumn(newColumnName string, size uint8) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeShortBytesColumn{
		name: newColumnName,
		size: size,
	})
}

// LongBytesのカラムを追加する。
// 値はGoの[]byteとして扱われる。
// 0～65535バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
func (tc *TableCreator) LongBytesColumn(newColumnName string) error {
	return tc.addColumn(&longBytesColumn{
		name: newColumnName,
	})
}

// FixedSizeLongBytesのカラムを追加する。
// sizeは1～65535の範囲の中から指定する。
// 値はGoの[]byteとして扱われる。
// テーブル作成時に指定した固定バイトサイズ（1～65535バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう値0が埋められる。
func (tc *TableCreator) FixedSizeLongBytesColumn(newColumnName string, size uint16) error {
	if size == 0 {
		return SizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeLongBytesColumn{
		name: newColumnName,
		size: size,
	})
}

// Blobのカラムを追加する。
// 値はGoの[]byteとして扱われる。
// 0～1073741823バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
func (tc *TableCreator) BlobColumn(newColumnName string) error {
	return tc.addColumn(&blobColumn{
		name: newColumnName,
	})
}
