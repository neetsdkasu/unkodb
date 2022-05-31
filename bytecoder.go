// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"encoding/binary"
	"fmt"
	"io"
)

type ByteEncoder struct {
	writer io.Writer
	order  binary.ByteOrder
}

type ByteDecoder struct {
	reader io.Reader
	order  binary.ByteOrder
}

func NewByteEncoder(writer io.Writer, order binary.ByteOrder) *ByteEncoder {
	return &ByteEncoder{writer, order}
}

func NewByteDecoder(reader io.Reader, order binary.ByteOrder) *ByteDecoder {
	return &ByteDecoder{reader, order}
}

func (encoder *ByteEncoder) RawBytes(data []byte) error {
	n, err := encoder.writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("cannot write data (length: %d, wrote: %d)", len(data), n)
	}
	return nil
}

func (decoder *ByteDecoder) RawBytes(buffer []byte) error {
	n, err := io.ReadFull(decoder.reader, buffer)
	if err != nil {
		return err
	}
	if n != len(buffer) {
		return fmt.Errorf("cannot read data (length: %d, read: %d)", len(buffer), n)
	}
	return nil
}

func (encoder *ByteEncoder) Value(src any) error {
	return binary.Write(encoder.writer, encoder.order, src)
}

func (decoder *ByteDecoder) Value(dst any) error {
	return binary.Read(decoder.reader, decoder.order, dst)
}

func (encoder *ByteEncoder) Uint16(src uint16) error {
	return encoder.Value(src)
}

func (decoder *ByteDecoder) Uint16(dst *uint16) error {
	return decoder.Value(dst)
}

func (encoder *ByteEncoder) Int32(src int32) error {
	return encoder.Value(src)
}

func (decoder *ByteDecoder) Int32(dst *int32) error {
	return decoder.Value(dst)
}
