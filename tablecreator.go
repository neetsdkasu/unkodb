// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

// 新しいテーブルの作成に使用される。
//
//	var tc *TableCreator
//	tc, _ = db.CreateTable("my_book_table")
//	tc.CounterKey("id")
//	tc.ShortStringColumn("title")
//	tc.ShortStringColumn("author")
//	tc.ShortStringColumn("genre")
//	table, _ := tc.Create()
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
// キーが設定されてないなどがある場合に対応したエラーが返る。
// それ以外のエラー（IOエラーなど）がある場合にも戻り値エラーにはnil以外が返る。（たいていプログラムの実行に致命的なエラー）。
//
//	var tc *TableCreator
//	tc, _ = db.CreateTable("my_book_table")
//	tc.CounterKey("id")
//	tc.ShortStringColumn("title")
//	tc.ShortStringColumn("author")
//	tc.ShortStringColumn("genre")
//	table, _ := tc.Create()
func (tc *TableCreator) Create() (table *Table, err error) {
	if !debugMode {
		defer catchError(&err)
	}
	if tc.created {
		err = ErrInvalidOperation
		return
	}
	if tc.key == nil {
		err = ErrNeedToSetAKey
		return
	}
	var dataSize uint64 = 0
	for _, col := range tc.columns {
		dataSize += col.MaximumDataByteSize()
	}
	var dataSeparation dataSeparationState
	if dataSize <= noSeparationMaximumDataSize {
		dataSeparation = dataSeparationDisabled
	} else {
		dataSeparation = dataSeparationEnabled
	}
	table, err = tc.db.newTable(tc.name, tc.key, tc.columns, dataSeparation)
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
		return ErrInvalidOperation
	}
	if tc.key != nil {
		return ErrKeyAlreadyExists
	}
	// TODO カラム名の文字構成チェックいる？？
	if len([]byte(column.Name())) == 0 {
		return ErrNeedColumnName
	}
	if len([]byte(column.Name())) > MaximumColumnNameByteSize {
		return ErrColumnNameIsTooLong
	}
	if tc.has(column.Name()) {
		return ErrColumnNameAlreadyExists
	}
	tc.key = column
	tc.columnNameMap[column.Name()] = true
	return nil
}

func (tc *TableCreator) addColumn(column Column) error {
	if tc.created {
		return ErrInvalidOperation
	}
	if len(tc.columns) >= MaximumColumnCountWithoutKey {
		return ErrColumnCountIsFull
	}
	// TODO カラム名の文字構成チェックいる？？
	if len([]byte(column.Name())) == 0 {
		return ErrNeedColumnName
	}
	if len([]byte(column.Name())) > MaximumColumnNameByteSize {
		return ErrColumnNameIsTooLong
	}
	if tc.has(column.Name()) {
		return ErrColumnNameAlreadyExists
	}
	tc.columns = append(tc.columns, column)
	tc.columnNameMap[column.Name()] = true
	return nil
}

// Int8のキーを設定する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Int8Key(newColumnName string) error {
	return tc.setKey(&intColumn[int8]{
		name: newColumnName,
	})
}

// Int8のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Int8Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int8]{
		name: newColumnName,
	})
}

// Uint8のキーを設定する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Uint8Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint8]{
		name: newColumnName,
	})
}

// Uint8のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Uint8Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint8]{
		name: newColumnName,
	})
}

// Int16のキーを設定する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Int16Key(newColumnName string) error {
	return tc.setKey(&intColumn[int16]{
		name: newColumnName,
	})
}

// Int16のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Int16Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int16]{
		name: newColumnName,
	})
}

// Uint16のキーを設定する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Uint16Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint16]{
		name: newColumnName,
	})
}

// Uint16のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Uint16Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint16]{
		name: newColumnName,
	})
}

// Int32のキーを設定する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Int32Key(newColumnName string) error {
	return tc.setKey(&intColumn[int32]{
		name: newColumnName,
	})
}

// Int32のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Int32Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int32]{
		name: newColumnName,
	})
}

// Uint32のキーを設定する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Uint32Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint32]{
		name: newColumnName,
	})
}

// Uint32のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Uint32Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint32]{
		name: newColumnName,
	})
}

// Int64のキーを設定する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Int64Key(newColumnName string) error {
	return tc.setKey(&intColumn[int64]{
		name: newColumnName,
	})
}

// Int64のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Int64Column(newColumnName string) error {
	return tc.addColumn(&intColumn[int64]{
		name: newColumnName,
	})
}

// Uint64のキーを設定する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Uint64Key(newColumnName string) error {
	return tc.setKey(&intColumn[uint64]{
		name: newColumnName,
	})
}

// Uint64のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Uint64Column(newColumnName string) error {
	return tc.addColumn(&intColumn[uint64]{
		name: newColumnName,
	})
}

// Counterのキーを設定する。
// 値はGoのuint32の型として扱われる。
// データが挿入時にファイルにCounterの値が書き込まれる。データの挿入ごとに1ずつ値が増えていく（最初は1から始まる）。unkodb.CounterTypeはuint32のエイリアス。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) CounterKey(newColumnName string) error {
	return tc.setKey(&counterColumn{
		name: newColumnName,
	})
}

// Float32のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Float32Column(newColumnName string) error {
	return tc.addColumn(&floatColumn[float32]{
		name: newColumnName,
	})
}

// Float64のカラムを追加する。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) Float64Column(newColumnName string) error {
	return tc.addColumn(&floatColumn[float64]{
		name: newColumnName,
	})
}

// ShortStringのキーを設定する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。0～255バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。キーとして使う場合は`strings.Compare`が順序に使用される。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) ShortStringKey(newColumnName string) error {
	return tc.setKey(&shortStringColumn{
		name: newColumnName,
	})
}

// ShortStringのカラムを追加する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。0～255バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) ShortStringColumn(newColumnName string) error {
	return tc.addColumn(&shortStringColumn{
		name: newColumnName,
	})
}

// FixedSizeShortStringのキーを設定する。
// sizeは1～255の範囲の中から指定する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。テーブル作成時に指定した固定バイトサイズ（1～255バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。キーとして使う場合は`strings.Compare`が順序に使用される。
// カラム名やサイズ指定に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) FixedSizeShortStringKey(newColumnName string, size uint8) error {
	if size == 0 {
		return ErrSizeMustBePositiveValue
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
// カラム名やサイズ指定に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) FixedSizeShortStringColumn(newColumnName string, size uint8) error {
	if size == 0 {
		return ErrSizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeShortStringColumn{
		name: newColumnName,
		size: size,
	})
}

// LongStringのカラムを追加する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。0～65535バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) LongStringColumn(newColumnName string) error {
	return tc.addColumn(&longStringColumn{
		name: newColumnName,
	})
}

// FixedSizeLongStringのカラムを追加する。
// sizeは1～65535の範囲の中から指定する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。テーブル作成時に指定した固定バイトサイズ（1～65535バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。
// カラム名やサイズ指定に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) FixedSizeLongStringColumn(newColumnName string, size uint16) error {
	if size == 0 {
		return ErrSizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeLongStringColumn{
		name: newColumnName,
		size: size,
	})
}

// Textのカラムを追加する。
// 値はGoのstringとして扱われる。
// 内部的にはstringを[]byteキャストした形で保存される。0～1073741823バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) TextColumn(newColumnName string) error {
	return tc.addColumn(&textColumn{
		name: newColumnName,
	})
}

// ShortBytesのキーを設定する。
// 値はGoの[]byteとして扱われる。
// 0～255バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。キーとして使う場合はbytes.Compareが順序に使用される。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) ShortBytesKey(newColumnName string) error {
	return tc.setKey(&shortBytesColumn{
		name: newColumnName,
	})
}

// ShortBytesのカラムを追加する。
// 値はGoの[]byteとして扱われる。
// 0～255バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) ShortBytesColumn(newColumnName string) error {
	return tc.addColumn(&shortBytesColumn{
		name: newColumnName,
	})
}

// FixedSizeShortBytesのキーを設定する。
// sizeは1～255の範囲の中から指定する。
// 値はGoの[]byteとして扱われる。
// テーブル作成時に指定した固定バイトサイズ（1～255バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう半角スペースが埋められる。キーとして使う場合は`bytes.Compare`が順序に使用される。
// カラム名やサイズ指定に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) FixedSizeShortBytesKey(newColumnName string, size uint8) error {
	if size == 0 {
		return ErrSizeMustBePositiveValue
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
// カラム名やサイズ指定に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) FixedSizeShortBytesColumn(newColumnName string, size uint8) error {
	if size == 0 {
		return ErrSizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeShortBytesColumn{
		name: newColumnName,
		size: size,
	})
}

// LongBytesのカラムを追加する。
// 値はGoの[]byteとして扱われる。
// 0～65535バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) LongBytesColumn(newColumnName string) error {
	return tc.addColumn(&longBytesColumn{
		name: newColumnName,
	})
}

// FixedSizeLongBytesのカラムを追加する。
// sizeは1～65535の範囲の中から指定する。
// 値はGoの[]byteとして扱われる。
// テーブル作成時に指定した固定バイトサイズ（1～65535バイト）で保存される。サイズ未満の文字列の場合、指定バイトサイズになるよう値0が埋められる。
// カラム名やサイズ指定に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) FixedSizeLongBytesColumn(newColumnName string, size uint16) error {
	if size == 0 {
		return ErrSizeMustBePositiveValue
	}
	return tc.addColumn(&fixedSizeLongBytesColumn{
		name: newColumnName,
		size: size,
	})
}

// Blobのカラムを追加する。
// 値はGoの[]byteとして扱われる。
// 0～1073741823バイトに収まる必要がある。バイト長もデータごとに一緒に保存される。
// カラム名に不正がある場合に対応したエラーが返る。
func (tc *TableCreator) BlobColumn(newColumnName string) error {
	return tc.addColumn(&blobColumn{
		name: newColumnName,
	})
}
