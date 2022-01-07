package unkodb

type KeyType int

const (
	CountId KeyType = iota
	Int32Key
	Int32Id
	Uint32Key
	Uint32Id
	Int64Key
	Int64Id
	Uint64Key
	Uint64Id
	BytesKey
	BytesId
	StringKey
	StringId
)

type ColumnType int

const (
	BooleanColumnType ColumnType = iota
	Int8ColumnType
	Uint8ColumnType
	Int16ColumnType
	Uint16ColumnType
	Int32ColumnType
	Uint32ColumnType
	Int64ColumnType
	Uint64ColumnType
	Float32CloumnType
	Float64CloumnType
	FixedBytesColumnType
	DynamicBytesColumnType
	FixedStringColumnType
	DynamicStringColumnType
)

type ColumnSpec interface {
	Name() string
	Type() ColumnType
}

type columnSpec struct {
	name       string
	columnType ColumnType
}

type Table interface {
	Name() string
	Columns() []ColumnSpec
	spec() *tableSpec
}

type tableSpec struct {
	address     int
	name        string
	keyType     KeyType
	keyName     string
	rootEntry   int // ルートのインデックス
	entryCount  int // 要素数
	nextEntryId int // CountIdのときのみ使用
	columns     []*columnSpec
}
