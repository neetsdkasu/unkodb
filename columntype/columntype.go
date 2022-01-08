package columntype

type ColumnType int

const (
	Boolean ColumnType = iota
	Int8
	Uint8
	Int16
	Uint16
	Int32
	Uint32
	Int64
	Uint64
	Float32
	Float64
	Bytes
	String
)
