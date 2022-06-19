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
