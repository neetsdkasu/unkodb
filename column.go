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

	// データ領域の最小バイトサイズ
	MinimumDataByteSize() int

	// データ領域の最大バイトサイズ
	MaximumDataByteSize() int

	// レコードバッファに書き込む際のバイトサイズ(データのバイトサイズとメタ情報があるならそのバイトサイズとの合計サイズ)
	byteSizeHint(value any) int

	// レコードバッファからのデータの読み込み
	read(decoder *byteDecoder) (value any, err error)

	// レコードバッファへのデータの書き込み
	write(encoder *byteEncoder, value any) (err error)
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

func (*int8Column) MinimumDataByteSize() int {
	return 1
}

func (*int8Column) MaximumDataByteSize() int {
	return 1
}

func (*int8Column) byteSizeHint(value any) int {
	if _, ok := value.(int8); ok {
		return 1
	} else {
		logger.Panicf("[BUG] value type is not int8 (value: %T %#v)", value, value)
		return 0
	}
}

func (*int8Column) read(decoder *byteDecoder) (value any, err error) {
	var v int8
	err = decoder.Int8(&v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (*int8Column) write(encoder *byteEncoder, value any) (err error) {
	if v, ok := value.(int8); ok {
		err = encoder.Int8(v)
	} else {
		logger.Panicf("[BUG] value type is not int8 (value: %T %#v)", value, value)
	}
	return
}

type shortStringColumn struct {
	name string
}

func (c *shortStringColumn) Name() string {
	return c.name
}

func (*shortStringColumn) Type() ColumnType {
	return ShortString
}

func (*shortStringColumn) MinimumDataByteSize() int {
	return 0
}

func (*shortStringColumn) MaximumDataByteSize() int {
	return 255
}

func (*shortStringColumn) byteSizeHint(value any) int {
	if s, ok := value.(string); ok {
		return minValue(255, len([]byte(s))) + 1
	} else {
		logger.Panicf("[BUG] value type is not string (value: %T %#v)", value, value)
		return 0
	}
}

func (*shortStringColumn) read(decoder *byteDecoder) (value any, err error) {
	var size uint8
	err = decoder.Uint8(&size)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	err = decoder.RawBytes(buf)
	if err != nil {
		return nil, err
	}
	s := string(buf)
	return s, nil
}

func (*shortStringColumn) write(encoder *byteEncoder, value any) (err error) {
	if s, ok := value.(string); ok {
		buf := []byte(s)
		if len(buf) > 255 {
			buf = buf[:255]
		}
		err = encoder.Uint8(uint8(len(buf)))
		if err != nil {
			return
		}
		err = encoder.RawBytes(buf)
	} else {
		logger.Panicf("[BUG] value type is not string (value: %T %#v)", value, value)
	}
	return
}
