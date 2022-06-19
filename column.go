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
		logger.Panic("[BUG] Unreachable")
		return
	}
}

func (*intColumn[T]) MinimumDataByteSize() int {
	return int(unsafe.Sizeof(T(0)))
}

func (*intColumn[T]) MaximumDataByteSize() int {
	return int(unsafe.Sizeof(T(0)))
}

func (*intColumn[T]) byteSizeHint(value any) (_ int) {
	if _, ok := value.(T); ok {
		return int(unsafe.Sizeof(T(0)))
	} else {
		logger.Panicf("[BUG] value type is not %T (value: %T %#v)", T(0), value, value)
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
		logger.Panicf("[BUG] value type is not %T (value: %T %#v)", T(0), value, value)
	}
	return
}

func (*intColumn[T]) toKey(value any) avltree.Key {
	if v, ok := value.(T); ok {
		return intKey[T](v)
	} else {
		logger.Panicf("[BUG] value type is not %T (value: %T %#v)", T(0), value, value)
		return nil
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

func (*shortStringColumn) MinimumDataByteSize() int {
	return shortStringMinimumDataByteSize
}

func (*shortStringColumn) MaximumDataByteSize() int {
	return shortStringMaximumDataByteSize
}

func (*shortStringColumn) byteSizeHint(value any) int {
	if s, ok := value.(string); ok {
		return minValue(shortStringMaximumDataByteSize, len([]byte(s))) + 1
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
		if len(buf) > shortStringMaximumDataByteSize {
			buf = buf[:shortStringMaximumDataByteSize]
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

func (*shortStringColumn) toKey(value any) avltree.Key {
	if s, ok := value.(string); ok {
		return stringkey.StringKey(s)
	} else {
		logger.Panicf("[BUG] value type is not string (value: %T %#v)", value, value)
		return nil
	}
}
