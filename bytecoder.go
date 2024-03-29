// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"encoding/binary"
	"fmt"
	"io"
)

type byteSliceWriter struct {
	buf []byte
}

type byteEncoder struct {
	writer io.Writer
	order  binary.ByteOrder
}

type byteDecoder struct {
	reader io.Reader
	order  binary.ByteOrder
}

func newByteSliceWriter(buf []byte) *byteSliceWriter {
	return &byteSliceWriter{buf[:0:len(buf)]}
}

func newByteEncoder(writer io.Writer, order binary.ByteOrder) *byteEncoder {
	return &byteEncoder{writer, order}
}

func newByteDecoder(reader io.Reader, order binary.ByteOrder) *byteDecoder {
	return &byteDecoder{reader, order}
}

func (w *byteSliceWriter) Write(p []byte) (n int, err error) {
	buf := w.buf
	if len(buf) == cap(buf) {
		err = io.EOF
		return
	}
	if len(buf)+len(p) > cap(buf) {
		p = p[:cap(buf)-len(buf)]
		err = io.ErrShortWrite
	}
	buf = append(buf, p...)
	n = len(p)
	w.buf = buf
	return
}

func (w *byteSliceWriter) Buffer() []byte {
	return w.buf
}

func (encoder *byteEncoder) RawBytes(data []byte) error {
	n, err := encoder.writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("cannot write data (length: %d, wrote: %d)", len(data), n)
	}
	return nil
}

func (decoder *byteDecoder) RawBytes(buffer []byte) error {
	n, err := io.ReadFull(decoder.reader, buffer)
	if err != nil {
		return err
	}
	if n != len(buffer) {
		return fmt.Errorf("cannot read data (length: %d, read: %d)", len(buffer), n)
	}
	return nil
}

func (encoder *byteEncoder) Value(src any) error {
	return binary.Write(encoder.writer, encoder.order, src)
}

func (decoder *byteDecoder) Value(dst any) error {
	return binary.Read(decoder.reader, decoder.order, dst)
}

func (encoder *byteEncoder) Int8(src int8) error {
	return encoder.Value(src)
}

func (decoder *byteDecoder) Int8(dst *int8) error {
	return decoder.Value(dst)
}

func (encoder *byteEncoder) Uint8(src uint8) error {
	return encoder.Value(src)
}

func (decoder *byteDecoder) Uint8(dst *uint8) error {
	return decoder.Value(dst)
}

func (encoder *byteEncoder) Uint16(src uint16) error {
	return encoder.Value(src)
}

func (decoder *byteDecoder) Uint16(dst *uint16) error {
	return decoder.Value(dst)
}

func (encoder *byteEncoder) Int32(src int32) error {
	return encoder.Value(src)
}

func (decoder *byteDecoder) Int32(dst *int32) error {
	return decoder.Value(dst)
}

func (encoder *byteEncoder) Uint32(src uint32) error {
	return encoder.Value(src)
}

func (decoder *byteDecoder) Uint32(dst *uint32) error {
	return decoder.Value(dst)
}

func (encoder *byteEncoder) WriteShortString(s string) (err error) {
	buf := []byte(s)
	if len(buf) > shortStringMaximumDataByteSize {
		buf = buf[:shortStringMaximumDataByteSize]
	}
	size := uint8(len(buf))
	err = encoder.Uint8(size)
	if err != nil {
		return
	}
	err = encoder.RawBytes(buf)
	return
}

func (decoder *byteDecoder) ReadShortString() (s string, err error) {
	var size uint8
	err = decoder.Uint8(&size)
	if err != nil {
		return
	}
	buf := make([]byte, size)
	err = decoder.RawBytes(buf)
	if err != nil {
		return
	}
	s = string(buf)
	return
}

func (encoder *byteEncoder) WriteColumnSpec(col Column) (err error) {
	err = encoder.WriteShortString(col.Name())
	if err != nil {
		return
	}
	err = encoder.Uint8(uint8(col.Type()))
	if err != nil {
		return
	}
	switch c := col.(type) {
	case *fixedSizeShortStringColumn:
		err = encoder.Uint8(c.size)
	case *fixedSizeLongStringColumn:
		err = encoder.Uint16(c.size)
	case *fixedSizeShortBytesColumn:
		err = encoder.Uint8(c.size)
	case *fixedSizeLongBytesColumn:
		err = encoder.Uint16(c.size)
	}
	return
}

func (decoder *byteDecoder) ReadColumnSpec() (col Column, err error) {
	var name string
	name, err = decoder.ReadShortString()
	if err != nil {
		return
	}
	var colType uint8
	err = decoder.Uint8(&colType)
	if err != nil {
		return
	}
	switch ColumnType(colType) {
	default:
		err = &ErrWrongFileFormat{"Unknown ColumnType"}
	case Counter:
		col = &counterColumn{name: name}
	case Int8:
		col = &intColumn[int8]{name: name}
	case Uint8:
		col = &intColumn[uint8]{name: name}
	case Int16:
		col = &intColumn[int16]{name: name}
	case Uint16:
		col = &intColumn[uint16]{name: name}
	case Int32:
		col = &intColumn[int32]{name: name}
	case Uint32:
		col = &intColumn[uint32]{name: name}
	case Int64:
		col = &intColumn[int64]{name: name}
	case Uint64:
		col = &intColumn[uint64]{name: name}
	case Float32:
		col = &floatColumn[float32]{name: name}
	case Float64:
		col = &floatColumn[float64]{name: name}
	case ShortString:
		col = &shortStringColumn{name: name}
	case FixedSizeShortString:
		// TODO サイズの値は正の整数が必要だけど0を読み込んだ場合の対処は？
		var size uint8
		err = decoder.Uint8(&size)
		if err != nil {
			return
		}
		col = &fixedSizeShortStringColumn{
			name: name,
			size: size,
		}
	case LongString:
		col = &longStringColumn{name: name}
	case FixedSizeLongString:
		// TODO サイズの値は正の整数が必要だけど0を読み込んだ場合の対処は？
		var size uint16
		err = decoder.Uint16(&size)
		if err != nil {
			return
		}
		col = &fixedSizeLongStringColumn{
			name: name,
			size: size,
		}
	case Text:
		col = &textColumn{name: name}
	case ShortBytes:
		col = &shortBytesColumn{name: name}
	case FixedSizeShortBytes:
		// TODO サイズの値は正の整数が必要だけど0を読み込んだ場合の対処は？
		var size uint8
		err = decoder.Uint8(&size)
		if err != nil {
			return
		}
		col = &fixedSizeShortBytesColumn{
			name: name,
			size: size,
		}
	case LongBytes:
		col = &longBytesColumn{name: name}
	case FixedSizeLongBytes:
		// TODO サイズの値は正の整数が必要だけど0を読み込んだ場合の対処は？
		var size uint16
		err = decoder.Uint16(&size)
		if err != nil {
			return
		}
		col = &fixedSizeLongBytesColumn{
			name: name,
			size: size,
		}
	case Blob:
		col = &blobColumn{name: name}
	}
	return
}
