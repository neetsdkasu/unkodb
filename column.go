// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"fmt"
	"unsafe"

	"github.com/neetsdkasu/avltree"
	"github.com/neetsdkasu/avltree/stringkey"
)

type CounterType = uint32

type ColumnType int

func (ct ColumnType) String() string {
	switch ct {
	default:
		return fmt.Sprint("Invalid(", int(ct), ")")
	case Counter:
		return "Counter"
	case Int8:
		return "Int8"
	case Uint8:
		return "Uint8"
	case Int16:
		return "Int16"
	case Uint16:
		return "Uint16"
	case Int32:
		return "Int32"
	case Uint32:
		return "Uint32"
	case Int64:
		return "Int64"
	case Uint64:
		return "Uint64"
	case Float32:
		return "Float32"
	case Float64:
		return "Float64"
	case ShortString:
		return "ShortString"
	case FixedSizeShortString:
		return "FixedSizeShortString"
	case LongString:
		return "LongString"
	case FixedSizeLongString:
		return "FixedSizeLongString"
	case Text:
		return "Text"
	case ShortBytes:
		return "ShortBytes"
	case FixedSizeShortBytes:
		return "FixedSizeShortBytes"
	case LongBytes:
		return "LongBytes"
	case FixedSizeLongBytes:
		return "FixedSizeLongBytes"
	case Blob:
		return "Blob"
	}
}

func (ct ColumnType) GoTypeHint() string {
	switch ct {
	default:
		return fmt.Sprint("Invalid(", int(ct), ")")
	case Counter:
		return "uint32"
	case Int8:
		return "int8"
	case Uint8:
		return "uint8"
	case Int16:
		return "int16"
	case Uint16:
		return "uint16"
	case Int32:
		return "int32"
	case Uint32:
		return "uint32"
	case Int64:
		return "int64"
	case Uint64:
		return "uint64"
	case Float32:
		return "float32"
	case Float64:
		return "float64"
	case ShortString:
		return "string"
	case FixedSizeShortString:
		return "string"
	case LongString:
		return "string"
	case FixedSizeLongString:
		return "string"
	case Text:
		return "string"
	case ShortBytes:
		return "[]byte"
	case FixedSizeShortBytes:
		return "[]byte"
	case LongBytes:
		return "[]byte"
	case FixedSizeLongBytes:
		return "[]byte"
	case Blob:
		return "[]byte"
	}
}

func (ct ColumnType) keyColumnType() bool {
	switch ct {
	default:
		return false
	case Counter:
		return true
	case Int8:
		return true
	case Uint8:
		return true
	case Int16:
		return true
	case Uint16:
		return true
	case Int32:
		return true
	case Uint32:
		return true
	case Int64:
		return true
	case Uint64:
		return true
	case ShortString:
		return true
	case FixedSizeShortString:
		return true
	case ShortBytes:
		return true
	case FixedSizeShortBytes:
		return true
	}
}

func ColumnTypeHint(col Column) string {
	ct := col.Type()
	switch ct {
	default:
		return ct.String() + " (" + ct.GoTypeHint() + ")"
	case FixedSizeShortString:
		size := col.(*fixedSizeShortStringColumn).size
		return fmt.Sprint(ct.String(), "[", size, "] (", ct.GoTypeHint(), ")")
	case FixedSizeLongString:
		size := col.(*fixedSizeLongStringColumn).size
		return fmt.Sprint(ct.String(), "[", size, "] (", ct.GoTypeHint(), ")")
	case FixedSizeShortBytes:
		size := col.(*fixedSizeShortBytesColumn).size
		return fmt.Sprint(ct.String(), "[", size, "] (", ct.GoTypeHint(), ")")
	case FixedSizeLongBytes:
		size := col.(*fixedSizeLongBytesColumn).size
		return fmt.Sprint(ct.String(), "[", size, "] (", ct.GoTypeHint(), ")")
	}
}

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

	// データコピーを生成
	copyValue(value any) (copiedVale any)
}

type keyColumn interface {
	Column

	// キーに変換する
	toKey(value any) avltree.Key

	unwrapKey(key avltree.Key) (value any)
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

func (*intColumn[T]) copyValue(value any) any {
	return value
}

func (*intColumn[T]) toKey(value any) (_ avltree.Key) {
	if v, ok := value.(T); ok {
		return intKey[T](v)
	} else {
		bug.Panicf("intColumn.toKey: value type is not %T (value: %T %#v)", T(0), value, value)
		return
	}
}

func (*intColumn[T]) unwrapKey(key avltree.Key) (_ any) {
	if k, ok := key.(*geneKey[T]); ok {
		return k.value
	} else {
		bug.Panic("key is not *geneKey[T]")
		return
	}
}

type counterColumn struct {
	name string
}

func (c *counterColumn) Name() string {
	return c.name
}

func (*counterColumn) Type() ColumnType {
	return Counter
}

func (*counterColumn) IsValidValueType(value any) (ok bool) {
	_, ok = value.(uint32)
	return
}

func (*counterColumn) MinimumDataByteSize() uint64 {
	return uint64(unsafe.Sizeof(uint32(0)))
}

func (*counterColumn) MaximumDataByteSize() uint64 {
	return uint64(unsafe.Sizeof(uint32(0)))
}

func (*counterColumn) byteSizeHint(value any) (_ uint64) {
	if _, ok := value.(uint32); ok {
		return uint64(unsafe.Sizeof(uint32(0)))
	} else {
		bug.Panicf("counterColumn.byteSizeHint: value type is not uint32 (value: %T %#v)", value, value)
		return
	}
}

func (*counterColumn) read(decoder *byteDecoder) (value any, err error) {
	var counting uint32
	err = decoder.Uint32(&counting)
	if err != nil {
		return nil, err
	}
	return counting, nil
}

func (*counterColumn) write(encoder *byteEncoder, value any) (err error) {
	if v, ok := value.(uint32); ok {
		err = encoder.Uint32(uint32(v))
	} else {
		bug.Panicf("counterColumn.write: value type is not uint32 (value: %T %#v)", value, value)
	}
	return
}

func (*counterColumn) copyValue(value any) any {
	return value
}

func (*counterColumn) toKey(value any) (_ avltree.Key) {
	if v, ok := value.(uint32); ok {
		return intKey[uint32](v)
	} else {
		bug.Panicf("counterColumn.toKey: value type is not uint32 (value: %T %#v)", value, value)
		return
	}
}

func (*counterColumn) unwrapKey(key avltree.Key) (_ any) {
	if k, ok := key.(*geneKey[uint32]); ok {
		return k.value
	} else {
		bug.Panic("key is not geneKey")
		return
	}
}

// 構造的にint系と分ける意味は･･･toKeyがあるか、ないか、か？
type floatColumn[T float32 | float64] struct {
	name string
}

func (c *floatColumn[T]) Name() string {
	return c.name
}

func (*floatColumn[T]) Type() (_ ColumnType) {
	// アホっぽい
	switch any(T(0)).(type) {
	case float32:
		return Float32
	case float64:
		return Float64
	default:
		bug.Panic("floatColumn.Type: Unreachable")
		return
	}
}

func (*floatColumn[T]) IsValidValueType(value any) (ok bool) {
	_, ok = value.(T)
	return
}

func (*floatColumn[T]) MinimumDataByteSize() uint64 {
	return uint64(unsafe.Sizeof(T(0)))
}

func (*floatColumn[T]) MaximumDataByteSize() uint64 {
	return uint64(unsafe.Sizeof(T(0)))
}

func (*floatColumn[T]) byteSizeHint(value any) (_ uint64) {
	if _, ok := value.(T); ok {
		return uint64(unsafe.Sizeof(T(0)))
	} else {
		bug.Panicf("floatColumn.byteSizeHint: value type is not %T (value: %T %#v)", T(0), value, value)
		return
	}
}

func (*floatColumn[T]) read(decoder *byteDecoder) (value any, err error) {
	var v T
	err = decoder.Value(&v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (*floatColumn[T]) write(encoder *byteEncoder, value any) (err error) {
	if _, ok := value.(T); ok {
		err = encoder.Value(value)
	} else {
		bug.Panicf("floatColumn.write: value type is not %T (value: %T %#v)", T(0), value, value)
	}
	return
}

func (*floatColumn[T]) copyValue(value any) any {
	return value
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

func (*shortStringColumn) copyValue(value any) any {
	return value
}

func (*shortStringColumn) toKey(value any) (_ avltree.Key) {
	if s, ok := value.(string); ok {
		return stringkey.StringKey(s)
	} else {
		bug.Panicf("shortStringColumn.toKey: value type is not string (value: %T %#v)", value, value)
		return
	}
}

func (*shortStringColumn) unwrapKey(key avltree.Key) (_ any) {
	if s, ok := key.(stringkey.StringKey); ok {
		// サイズチェック必要？
		return string(s)
	} else {
		bug.Panic("key is not stringKey.StringKey")
		return
	}
}

type fixedSizeShortStringColumn struct {
	name string
	size uint8
}

func (c *fixedSizeShortStringColumn) Name() string {
	return c.name
}

func (*fixedSizeShortStringColumn) Type() ColumnType {
	return FixedSizeShortString
}

func (c *fixedSizeShortStringColumn) IsValidValueType(value any) bool {
	if s, ok := value.(string); ok {
		b := []byte(s)
		return len(b) <= int(c.size)
	} else {
		return false
	}
}

func (c *fixedSizeShortStringColumn) MinimumDataByteSize() uint64 {
	return uint64(c.size)
}

func (c *fixedSizeShortStringColumn) MaximumDataByteSize() uint64 {
	return uint64(c.size)
}

func (c *fixedSizeShortStringColumn) byteSizeHint(value any) (_ uint64) {
	if _, ok := value.(string); ok {
		return uint64(c.size)
	} else {
		bug.Panicf("fixedSizeShortStringColumn.byteSizeHint: value type is not string (value: %T %#v)", value, value)
		return
	}
}

func (c *fixedSizeShortStringColumn) read(decoder *byteDecoder) (value any, err error) {
	buf := make([]byte, c.size)
	err = decoder.RawBytes(buf)
	if err != nil {
		return nil, err
	}
	s := string(buf)
	return s, nil
}

func (c *fixedSizeShortStringColumn) write(encoder *byteEncoder, value any) (err error) {
	if s, ok := value.(string); ok {
		buf := []byte(s)
		if len(buf) > int(c.size) {
			buf = buf[:c.size]
		} else {
			for len(buf) < int(c.size) {
				buf = append(buf, ' ')
			}
		}
		err = encoder.RawBytes(buf)
	} else {
		bug.Panicf("fixedSizeShortStringColumn.write: value type is not string (value: %T %#v)", value, value)
	}
	return
}

func (*fixedSizeShortStringColumn) copyValue(value any) any {
	return value
}

func (c *fixedSizeShortStringColumn) toKey(value any) (_ avltree.Key) {
	if s, ok := value.(string); ok {
		buf := []byte(s)
		if len(buf) > int(c.size) {
			s = string(buf[:c.size])
		} else if len(buf) < int(c.size) {
			for len(buf) < int(c.size) {
				buf = append(buf, ' ')
			}
			s = string(buf)
		}
		return stringkey.StringKey(s)
	} else {
		bug.Panicf("fixedSizeShortStringColumn.toKey: value type is not string (value: %T %#v)", value, value)
		return
	}
}

func (*fixedSizeShortStringColumn) unwrapKey(key avltree.Key) (_ any) {
	if s, ok := key.(stringkey.StringKey); ok {
		// サイズチェック必要？
		return string(s)
	} else {
		bug.Panic("key is not stringKey.StringKey")
		return
	}
}

type longStringColumn struct {
	name string
}

func (c *longStringColumn) Name() string {
	return c.name
}

func (*longStringColumn) Type() ColumnType {
	return LongString
}

func (*longStringColumn) IsValidValueType(value any) bool {
	if s, ok := value.(string); ok {
		b := []byte(s)
		return len(b) <= longStringMaximumDataByteSize
	} else {
		return false
	}
}

func (*longStringColumn) MinimumDataByteSize() uint64 {
	return longStringMinimumDataByteSize
}

func (*longStringColumn) MaximumDataByteSize() uint64 {
	return longStringMaximumDataByteSize
}

func (*longStringColumn) byteSizeHint(value any) (_ uint64) {
	if s, ok := value.(string); ok {
		return uint64(minValue(longStringMaximumDataByteSize, len([]byte(s))) + longStringByteSizeDataLength)
	} else {
		bug.Panicf("longStringColumn.byteSizeHint: value type is not string (value: %T %#v)", value, value)
		return
	}
}

func (*longStringColumn) read(decoder *byteDecoder) (value any, err error) {
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
	s := string(buf)
	return s, nil
}

func (*longStringColumn) write(encoder *byteEncoder, value any) (err error) {
	if s, ok := value.(string); ok {
		buf := []byte(s)
		if len(buf) > longStringMaximumDataByteSize {
			buf = buf[:longStringMaximumDataByteSize]
		}
		err = encoder.Uint16(uint16(len(buf)))
		if err != nil {
			return
		}
		err = encoder.RawBytes(buf)
	} else {
		bug.Panicf("longStringColumn.write: value type is not string (value: %T %#v)", value, value)
	}
	return
}

func (*longStringColumn) copyValue(value any) any {
	return value
}

type fixedSizeLongStringColumn struct {
	name string
	size uint16
}

func (c *fixedSizeLongStringColumn) Name() string {
	return c.name
}

func (*fixedSizeLongStringColumn) Type() ColumnType {
	return FixedSizeLongString
}

func (c *fixedSizeLongStringColumn) IsValidValueType(value any) bool {
	if s, ok := value.(string); ok {
		b := []byte(s)
		return len(b) <= int(c.size)
	} else {
		return false
	}
}

func (c *fixedSizeLongStringColumn) MinimumDataByteSize() uint64 {
	return uint64(c.size)
}

func (c *fixedSizeLongStringColumn) MaximumDataByteSize() uint64 {
	return uint64(c.size)
}

func (c *fixedSizeLongStringColumn) byteSizeHint(value any) (_ uint64) {
	if _, ok := value.(string); ok {
		return uint64(c.size)
	} else {
		bug.Panicf("fixedSizeLongStringColumn.byteSizeHint: value type is not string (value: %T %#v)", value, value)
		return
	}
}

func (c *fixedSizeLongStringColumn) read(decoder *byteDecoder) (value any, err error) {
	buf := make([]byte, c.size)
	err = decoder.RawBytes(buf)
	if err != nil {
		return nil, err
	}
	s := string(buf)
	return s, nil
}

func (c *fixedSizeLongStringColumn) write(encoder *byteEncoder, value any) (err error) {
	if s, ok := value.(string); ok {
		buf := []byte(s)
		if len(buf) > int(c.size) {
			buf = buf[:c.size]
		} else {
			for len(buf) < int(c.size) {
				buf = append(buf, ' ')
			}
		}
		err = encoder.RawBytes(buf)
	} else {
		bug.Panicf("fixedSizeLongStringColumn.write: value type is not string (value: %T %#v)", value, value)
	}
	return
}

func (*fixedSizeLongStringColumn) copyValue(value any) any {
	return value
}

type textColumn struct {
	name string
}

func (c *textColumn) Name() string {
	return c.name
}

func (*textColumn) Type() ColumnType {
	return Text
}

func (*textColumn) IsValidValueType(value any) bool {
	if s, ok := value.(string); ok {
		b := []byte(s)
		return len(b) <= textMaximumDataByteSize
	} else {
		return false
	}
}

func (*textColumn) MinimumDataByteSize() uint64 {
	return textMinimumDataByteSize
}

func (*textColumn) MaximumDataByteSize() uint64 {
	return textMaximumDataByteSize
}

func (*textColumn) byteSizeHint(value any) (_ uint64) {
	if s, ok := value.(string); ok {
		return uint64(minValue(textMaximumDataByteSize, len([]byte(s))) + textByteSizeDataLength)
	} else {
		bug.Panicf("textColumn.byteSizeHint: value type is not string (value: %T %#v)", value, value)
		return
	}
}

func (*textColumn) read(decoder *byteDecoder) (value any, err error) {
	var size uint32
	err = decoder.Uint32(&size)
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

func (*textColumn) write(encoder *byteEncoder, value any) (err error) {
	if s, ok := value.(string); ok {
		buf := []byte(s)
		if len(buf) > textMaximumDataByteSize {
			buf = buf[:textMaximumDataByteSize]
		}
		err = encoder.Uint32(uint32(len(buf)))
		if err != nil {
			return
		}
		err = encoder.RawBytes(buf)
	} else {
		bug.Panicf("textColumn.write: value type is not string (value: %T %#v)", value, value)
	}
	return
}

func (*textColumn) copyValue(value any) any {
	return value
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

func (*shortBytesColumn) copyValue(value any) (_ any) {
	if s, ok := value.([]byte); ok {
		r := make([]byte, len(s))
		copy(r, s)
		return r
	} else {
		bug.Panicf("shortBytesColumn.copyValue: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (*shortBytesColumn) toKey(value any) (_ avltree.Key) {
	if s, ok := value.([]byte); ok {
		return bytesKey(s)
	} else {
		bug.Panicf("shortBytesColumn.toKey: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (*shortBytesColumn) unwrapKey(key avltree.Key) (_ any) {
	if s, ok := key.(bytesKey); ok {
		// サイズチェック必要？
		return []byte(s)
	} else {
		bug.Panic("key is not bytesKey")
		return
	}
}

type fixedSizeShortBytesColumn struct {
	name string
	size uint8
}

func (c *fixedSizeShortBytesColumn) Name() string {
	return c.name
}

func (*fixedSizeShortBytesColumn) Type() ColumnType {
	return FixedSizeShortBytes
}

func (c *fixedSizeShortBytesColumn) IsValidValueType(value any) bool {
	if b, ok := value.([]byte); ok {
		return len(b) <= int(c.size)
	} else {
		return false
	}
}

func (c *fixedSizeShortBytesColumn) MinimumDataByteSize() uint64 {
	return uint64(c.size)
}

func (c *fixedSizeShortBytesColumn) MaximumDataByteSize() uint64 {
	return uint64(c.size)
}

func (c *fixedSizeShortBytesColumn) byteSizeHint(value any) (_ uint64) {
	if _, ok := value.([]byte); ok {
		return uint64(c.size)
	} else {
		bug.Panicf("fixedSizeShortBytesColumn.byteSizeHint: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (c *fixedSizeShortBytesColumn) read(decoder *byteDecoder) (value any, err error) {
	buf := make([]byte, c.size)
	err = decoder.RawBytes(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *fixedSizeShortBytesColumn) write(encoder *byteEncoder, value any) (err error) {
	if buf, ok := value.([]byte); ok {
		tmp := make([]byte, c.size)
		copy(tmp, buf)
		err = encoder.RawBytes(tmp)
	} else {
		bug.Panicf("fixedSizeShortBytesColumn.write: value type is not []byte (value: %T %#v)", value, value)
	}
	return
}

func (c *fixedSizeShortBytesColumn) copyValue(value any) (_ any) {
	if s, ok := value.([]byte); ok {
		r := make([]byte, c.size)
		copy(r, s)
		return r
	} else {
		bug.Panicf("fixedSizeShortBytesColumn.copyValue: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (c *fixedSizeShortBytesColumn) toKey(value any) (_ avltree.Key) {
	if buf, ok := value.([]byte); ok {
		tmp := make([]byte, c.size)
		copy(tmp, buf)
		return bytesKey(tmp)
	} else {
		bug.Panicf("fixedSizeShortBytesColumn.toKey: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (*fixedSizeShortBytesColumn) unwrapKey(key avltree.Key) (_ any) {
	if s, ok := key.(bytesKey); ok {
		// サイズチェック必要？
		return []byte(s)
	} else {
		bug.Panic("key is not bytesKey")
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

func (*longBytesColumn) copyValue(value any) (_ any) {
	if s, ok := value.([]byte); ok {
		r := make([]byte, len(s))
		copy(r, s)
		return r
	} else {
		bug.Panicf("longBytesColumn.copyValue: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

type fixedSizeLongBytesColumn struct {
	name string
	size uint16
}

func (c *fixedSizeLongBytesColumn) Name() string {
	return c.name
}

func (*fixedSizeLongBytesColumn) Type() ColumnType {
	return FixedSizeLongBytes
}

func (c *fixedSizeLongBytesColumn) IsValidValueType(value any) bool {
	if b, ok := value.([]byte); ok {
		return len(b) <= int(c.size)
	} else {
		return false
	}
}

func (c *fixedSizeLongBytesColumn) MinimumDataByteSize() uint64 {
	return uint64(c.size)
}

func (c *fixedSizeLongBytesColumn) MaximumDataByteSize() uint64 {
	return uint64(c.size)
}

func (c *fixedSizeLongBytesColumn) byteSizeHint(value any) (_ uint64) {
	if _, ok := value.([]byte); ok {
		return uint64(c.size)
	} else {
		bug.Panicf("fixedSizeLongBytesColumn.byteSizeHint: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (c *fixedSizeLongBytesColumn) read(decoder *byteDecoder) (value any, err error) {
	buf := make([]byte, c.size)
	err = decoder.RawBytes(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *fixedSizeLongBytesColumn) write(encoder *byteEncoder, value any) (err error) {
	if buf, ok := value.([]byte); ok {
		tmp := make([]byte, c.size)
		copy(tmp, buf)
		err = encoder.RawBytes(tmp)
	} else {
		bug.Panicf("fixedSizeLongBytesColumn.write: value type is not []byte (value: %T %#v)", value, value)
	}
	return
}

func (c *fixedSizeLongBytesColumn) copyValue(value any) (_ any) {
	if s, ok := value.([]byte); ok {
		r := make([]byte, c.size)
		copy(r, s)
		return r
	} else {
		bug.Panicf("fixedSizeLongBytesColumn.copyValue: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

type blobColumn struct {
	name string
}

func (c *blobColumn) Name() string {
	return c.name
}

func (*blobColumn) Type() ColumnType {
	return Blob
}

func (*blobColumn) IsValidValueType(value any) bool {
	if b, ok := value.([]byte); ok {
		return len(b) <= blobMaximumDataByteSize
	} else {
		return false
	}
}

func (*blobColumn) MinimumDataByteSize() uint64 {
	return blobMinimumDataByteSize
}

func (*blobColumn) MaximumDataByteSize() uint64 {
	return blobMaximumDataByteSize
}

func (*blobColumn) byteSizeHint(value any) (_ uint64) {
	if s, ok := value.([]byte); ok {
		return uint64(minValue(blobMaximumDataByteSize, len(s)) + blobByteSizeDataLength)
	} else {
		bug.Panicf("blobColumn.byteSizeHint: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}

func (*blobColumn) read(decoder *byteDecoder) (value any, err error) {
	var size uint32
	err = decoder.Uint32(&size)
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

func (*blobColumn) write(encoder *byteEncoder, value any) (err error) {
	if buf, ok := value.([]byte); ok {
		if len(buf) > blobMaximumDataByteSize {
			buf = buf[:blobMaximumDataByteSize]
		}
		err = encoder.Uint32(uint32(len(buf)))
		if err != nil {
			return
		}
		err = encoder.RawBytes(buf)
	} else {
		bug.Panicf("blobColumn.write: value type is not []byte (value: %T %#v)", value, value)
	}
	return
}

func (*blobColumn) copyValue(value any) (_ any) {
	if s, ok := value.([]byte); ok {
		r := make([]byte, len(s))
		copy(r, s)
		return r
	} else {
		bug.Panicf("blobColumn.copyValue: value type is not []byte (value: %T %#v)", value, value)
		return
	}
}
