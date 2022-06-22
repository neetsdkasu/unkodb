// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"unsafe"

	"github.com/neetsdkasu/avltree"
	"github.com/neetsdkasu/avltree/stringkey"
)

type ColumnType int

type Column interface {
	// カラム名
	Name() string

	// カラムのデータ型
	Type() ColumnType

	IsValidValueType(value any) bool

	// データ領域の最小バイトサイズ
	MinimumDataByteSize() uint64

	// データ領域の最大バイトサイズ
	MaximumDataByteSize() uint64

	// レコードバッファに書き込む際のバイトサイズ(データのバイトサイズとメタ情報があるならそのバイトサイズとの合計サイズ)
	byteSizeHint(value any) uint64

	// レコードバッファからのデータの読み込み
	read(decoder *byteDecoder) (value any, err error)

	// レコードバッファへのデータの書き込み
	write(encoder *byteEncoder, value any) (err error)
}

type keyColumn interface {
	Column

	// キーに変換する
	toKey(value any) avltree.Key
}

type intColumn[T integerTypes] struct {
	name string
}

func (c *intColumn[T]) Name() string {
	return c.name
}

func (*intColumn[T]) Type() (_ ColumnType) {
	// アホっぽい
	switch any(T(0)).(type) {
	case int8:
		return Int8
	case uint8:
		return Uint8
	case int16:
		return Int16
	case uint16:
		return Uint16
	case int32:
		return Int32
	case uint32:
		return Uint32
	case int64:
		return Int64
	case uint64:
		return Uint64
	default:
		bug.Panic("intColumn.Type: Unreachable")
		return
	}
}

func (*intColumn[T]) IsValidValueType(value any) (ok bool) {
	_, ok = value.(T)
	return
}

func (*intColumn[T]) MinimumDataByteSize() uint64 {
	return uint64(unsafe.Sizeof(T(0)))
}

func (*intColumn[T]) MaximumDataByteSize() uint64 {
	return uint64(unsafe.Sizeof(T(0)))
}

func (*intColumn[T]) byteSizeHint(value any) (_ uint64) {
	if _, ok := value.(T); ok {
		return uint64(unsafe.Sizeof(T(0)))
	} else {
		bug.Panicf("intColumn.byteSizeHint: value type is not %T (value: %T %#v)", T(0), value, value)
		return
	}
}

func (*intColumn[T]) read(decoder *byteDecoder) (value any, err error) {
	var v T
	err = decoder.Value(&v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (*intColumn[T]) write(encoder *byteEncoder, value any) (err error) {
	if _, ok := value.(T); ok {
		err = encoder.Value(value)
	} else {
		bug.Panicf("intColumn.write: value type is not %T (value: %T %#v)", T(0), value, value)
	}
	return
}

func (*intColumn[T]) toKey(value any) (_ avltree.Key) {
	if v, ok := value.(T); ok {
		return intKey[T](v)
	} else {
		bug.Panicf("intColumn.toKey: value type is not %T (value: %T %#v)", T(0), value, value)
		return
	}
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

func (*shortStringColumn) IsValidValueType(value any) bool {
	if s, ok := value.(string); ok {
		b := []byte(s)
		return len(b) <= shortStringMaximumDataByteSize
	} else {
		return false
	}
}

func (*shortStringColumn) MinimumDataByteSize() uint64 {
	return shortStringMinimumDataByteSize
}

func (*shortStringColumn) MaximumDataByteSize() uint64 {
	return shortStringMaximumDataByteSize
}

func (*shortStringColumn) byteSizeHint(value any) (_ uint64) {
	if s, ok := value.(string); ok {
		return uint64(minValue(shortStringMaximumDataByteSize, len([]byte(s))) + shortStringByteSizeDataLength)
	} else {
		bug.Panicf("shortStringColumn.byteSizeHint: value type is not string (value: %T %#v)", value, value)
		return
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
		if len(buf) > shortStringMaximumDataByteSize {
			buf = buf[:shortStringMaximumDataByteSize]
		}
		err = encoder.Uint8(uint8(len(buf)))
		if err != nil {
			return
		}
		err = encoder.RawBytes(buf)
	} else {
		bug.Panicf("shortStringColumn.write: value type is not string (value: %T %#v)", value, value)
	}
	return
}

func (*shortStringColumn) toKey(value any) (_ avltree.Key) {
	if s, ok := value.(string); ok {
		return stringkey.StringKey(s)
	} else {
		bug.Panicf("shortStringColumn.toKey: value type is not string (value: %T %#v)", value, value)
		return
	}
}

type shortBytesColumn struct {
	name string
}

func (c *shortBytesColumn) Name() string {
	return c.name
}

func (*shortBytesColumn) Type() ColumnType {
	return ShortBytes
}

func (*shortBytesColumn) IsValidValueType(value any) bool {
	if b, ok := value.([]byte); ok {
		return len(b) <= shortBytesMaximumDataByteSize
	} else {
		return false
	}
}

func (*shortBytesColumn) MinimumDataByteSize() uint64 {
	return shortBytesMinimumDataByteSize
}

func (*shortBytesColumn) MaximumDataByteSize() uint64 {
	return shortBytesMaximumDataByteSize
}

func (*shortBytesColumn) byteSizeHint(value any) (_ uint64) {
	if s, ok := value.([]byte); ok {
		return uint64(minValue(shortBytesMaximumDataByteSize, len(s)) + shortBytesByteSizeDataLength)
	} else {
		bug.Panicf("shortBytesColumn.byteSizeHint: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (*shortBytesColumn) read(decoder *byteDecoder) (value any, err error) {
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
	return buf, nil
}

func (*shortBytesColumn) write(encoder *byteEncoder, value any) (err error) {
	if buf, ok := value.([]byte); ok {
		if len(buf) > shortBytesMaximumDataByteSize {
			buf = buf[:shortBytesMaximumDataByteSize]
		}
		err = encoder.Uint8(uint8(len(buf)))
		if err != nil {
			return
		}
		err = encoder.RawBytes(buf)
	} else {
		bug.Panicf("shortBytesColumn.write: value type is not []byte (value: %T %#v)", value, value)
	}
	return
}

func (*shortBytesColumn) toKey(value any) (_ avltree.Key) {
	if s, ok := value.([]byte); ok {
		return bytesKey(s)
	} else {
		bug.Panicf("shortBytesColumn.toKey: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

type longBytesColumn struct {
	name string
}

func (c *longBytesColumn) Name() string {
	return c.name
}

func (*longBytesColumn) Type() ColumnType {
	return LongBytes
}

func (*longBytesColumn) IsValidValueType(value any) bool {
	if b, ok := value.([]byte); ok {
		return len(b) <= longBytesMaximumDataByteSize
	} else {
		return false
	}
}

func (*longBytesColumn) MinimumDataByteSize() uint64 {
	return longBytesMinimumDataByteSize
}

func (*longBytesColumn) MaximumDataByteSize() uint64 {
	return longBytesMaximumDataByteSize
}

func (*longBytesColumn) byteSizeHint(value any) (_ uint64) {
	if s, ok := value.([]byte); ok {
		return uint64(minValue(longBytesMaximumDataByteSize, len(s)) + longBytesByteSizeDataLength)
	} else {
		bug.Panicf("longBytesColumn.byteSizeHint: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (*longBytesColumn) read(decoder *byteDecoder) (value any, err error) {
	var size uint16
	err = decoder.Uint16(&size)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	err = decoder.RawBytes(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (*longBytesColumn) write(encoder *byteEncoder, value any) (err error) {
	if buf, ok := value.([]byte); ok {
		if len(buf) > longBytesMaximumDataByteSize {
			buf = buf[:longBytesMaximumDataByteSize]
		}
		err = encoder.Uint16(uint16(len(buf)))
		if err != nil {
			return
		}
		err = encoder.RawBytes(buf)
	} else {
		bug.Panicf("longBytesColumn.write: value type is not []byte (value: %T %#v)", value, value)
	}
	return
}
