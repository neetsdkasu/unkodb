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
	// カラム名
	Name() string

	// カラムのデータ型
	Type() ColumnType

	// レコードバッファに書き込む際のバイトサイズ(データのバイトサイズとメタ情報があるならそのバイトサイズとの合計サイズ)
	sizeHint(value any) int

	// レコードバッファからのデータの読み込み
	read(decoder *ByteDecoder) (value any, err error)

	// レコードバッファへのデータの書き込み
	write(encoder *ByteEncoder, value any) (err error)
}

type int8Column struct {
	name string
}

func (c *int8Column) Name() string {
	return c.name
}

func (*int8Column) Type() ColumnType {
	return Int8
}

func (*int8Column) sizeHint(value any) int {
	if _, ok := value.(int8); ok {
		return 1
	} else {
		logger.Panic("[BUG] Unmatch value type", value)
		return 0
	}
}

func (*int8Column) read(decoder *ByteDecoder) (value any, err error) {
	var v int8
	err = decoder.Int8(&v)
	return v, err
}

func (*int8Column) write(encoder *ByteEncoder, value any) (err error) {
	if v, ok := value.(int8); ok {
		err = encoder.Int8(v)
	} else {
		logger.Panic("[BUG] Unmatch value type", value)
	}
	return
}
