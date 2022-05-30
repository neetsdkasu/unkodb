// unkodb
// author: Leonardone @ NEETSDKASU

//
// ファイルヘッダフォーマット
//  シグネチャ
///   16 byte
//      3 5 7 11 13 17 19 23 29 31 'U' 'N' 'K' 'O' 'D' 'B'
//  フォーマットバージョン番号 (1から始める、255行くことはないと思うが一応2byte確保)
//    2 byte
//  予備領域（後で追加で情報を置きたくなったときの情報を置く場所のメモリ位置（アドレス？）を入れる）
//    4 byte
//  テーブル一覧のルートノードを示すメモリ位置（アドレス？） (0の場合はテーブルなし)
//    4 byte
//  空き領域断片のルートノードを示すメモリ位置（アドレス？） (0の場合は断片なし)
//    4 byte
package unkodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var byteOrder = binary.BigEndian

const (
	AddressSize = 4

	FileHeaderSignaturePosition = 0
	FileHeaderSignatureLength   = 16

	FileHeaderFileFormatVersionPosition = FileHeaderSignaturePosition + FileHeaderSignatureLength
	FileHeaderFileFormatVersionLength   = 2

	FileHeaderPreserveAreaPosition = FileHeaderFileFormatVersionPosition + FileHeaderFileFormatVersionLength
	FileHeaderPreserveAreaLength   = AddressSize

	FileHeaderTableListRootAddressPosition = FileHeaderPreserveAreaPosition + FileHeaderPreserveAreaLength
	FileHeaderTableListRootAddressLength   = AddressSize

	FileHeaderIdleSegmentListRootAddressPosition = FileHeaderTableListRootAddressPosition + FileHeaderTableListRootAddressLength
	FileHeaderIdleSegmentListRootAddressLength   = AddressSize

	FileHeaderSize = FileHeaderIdleSegmentListRootAddressPosition + FileHeaderIdleSegmentListRootAddressLength

	SegmentHeaderSize = AddressSize
)

type File struct {
	inner io.ReadWriteSeeker
}

type Segment struct {
	file     *File
	position int
	buffer   []byte
}

func Signature() []byte {
	return []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
	}
}

func NewFile(file io.ReadWriteSeeker) (*File, error) {
	fileSize, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if fileSize == 0 {
		// empty file
		// TODO: create new db
	} else if fileSize < FileHeaderSize {
		return nil, fmt.Errorf("Wrong File Format")
	}
	newFile := &File{inner: file}
	if err = newFile.checkHeader(); err != nil {
		return nil, err
	}
	return newFile, nil
}

func (file *File) checkHeader() error {
	if err := file.checkSignature(); err != nil {
		return err
	}
	// TODO: check others
	return nil
}

func (file *File) checkSignature() error {
	buffer, err := file.Read(FileHeaderSignaturePosition, FileHeaderSignatureLength)
	if err != nil {
		return fmt.Errorf("Failed File.checkSignature [%w]", err)
	}
	if !bytes.Equal(buffer, Signature()) {
		return fmt.Errorf("Wrong Signature in File Header")
	}
	return nil
}

func (file *File) ReadToBuffer(position int, buffer []byte) error {
	if _, err := file.inner.Seek(int64(position), io.SeekStart); err != nil {
		return err
	}
	if n, err := io.ReadFull(file.inner, buffer); err != nil {
		return err
	} else if n != len(buffer) {
		return fmt.Errorf("Cannot Read (Position: %d, Length: %d, Read: %d)", position, len(buffer), n)
	}
	return nil
}

func (file *File) Read(position, length int) ([]byte, error) {
	buffer := make([]byte, length)
	err := file.ReadToBuffer(position, buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (file *File) Write(position int, data []byte) error {
	if _, err := file.inner.Seek(int64(position), io.SeekStart); err != nil {
		return err
	}
	if n, err := file.inner.Write(data); err != nil {
		return err
	} else if n != len(data) {
		return fmt.Errorf("Cannot Write (Position: %d, Data Length: %d, Wrote: %d)", position, len(data), n)
	}
	return nil
}

func (file *File) ReadSegment(position int) (*Segment, error) {
	var headerBuffer [SegmentHeaderSize]byte
	err := file.ReadToBuffer(position, headerBuffer[:])
	if err != nil {
		return nil, fmt.Errorf("Failed File.ReadSegment (read size) [%w]", err)
	}
	size := int(byteOrder.Uint32(headerBuffer[:]))
	buffer, err := file.Read(position, size)
	if err != nil {
		return nil, fmt.Errorf("Failed File.ReadSegment (read data) [%w]", err)
	}
	seg := &Segment{file, position, buffer}
	return seg, nil
}

func (file *File) ReadTableListRootAddress() (int, error) {
	var buffer [FileHeaderTableListRootAddressLength]byte
	err := file.ReadToBuffer(FileHeaderTableListRootAddressPosition, buffer[:])
	if err != nil {
		return 0, fmt.Errorf("Failed File.ReadTableListRootAddress [%w]", err)
	}
	address := int(byteOrder.Uint32(buffer[:]))
	return address, nil
}

func (file *File) WriteTableListRootAddress(newAddress int) error {
	panic("TODO")
}

func (seg *Segment) Position() int {
	return seg.position
}

func (seg *Segment) Buffer() []byte {
	return seg.buffer[SegmentHeaderSize:]
}

func (seg *Segment) Flush() error {
	err := seg.file.Write(seg.position, seg.buffer)
	if err != nil {
		return fmt.Errorf("Failed Segment.Flush [%w]", err)
	}
	return nil
}
