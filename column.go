// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

type ColumnType int

const (
	Int8 ColumnType = iota
	Uint8
	Int16
	Uint16
	Int32
	Uint32
	Int64
	Uint64
	ShortString
	FixedSizeShortString
	LongString
	FixedSizeLongString
	Text
	ShortBytes
	FixedSizeShortBytes
	LongBytes
	FixedSizeLongBytes
	Blob
)

type Column interface {
	Name() string
	Type() ColumnType
	SizeHint() int
	read(decoder *ByteDecoder) (value any, err error)
	write(encoder *ByteEncoder, value any) (err error)
}

type int8Column struct {
	name string
}
