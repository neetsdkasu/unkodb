// unkodb
// author: Leonardone @ NEETSDKASU

//
// ファイルフォーマット
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
	FileHeaderSignaturePosition = 0
	FileHeaderSignatureLength   = 16

	FileHeaderFileFormatVersionPosition = FileHeaderSignaturePosition + FileHeaderSignatureLength
	FileHeaderFileFormatVersionLength   = 4

	FileHeaderPreserveAreaPosition = FileHeaderFileFormatVersionPosition + FileHeaderFileFormatVersionLength
	FileHeaderPreserveAreaLength   = 4

	FileHeaderTableListRootAddressPosition = FileHeaderPreserveAreaPosition + FileHeaderPreserveAreaLength
	FileHeaderTableListRootAddressLength   = 4

	FileHeaderIdleSegmentListRootAddressPosition = FileHeaderTableListRootAddressPosition +
		FileHeaderTableListRootAddressLength
	FileHeaderIdleSegmentListRootAddressLength = 4

	FileHeaderSize = FileHeaderIdleSegmentListRootAddressPosition +
		FileHeaderIdleSegmentListRootAddressLength
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
	buffer, err := file.Read(FileHeaderSignaturePosition,
		FileHeaderSignatureLength)
	if err != nil {
		return err
	}
	if !bytes.Equal(buffer, Signature()) {
		return fmt.Errorf("Wrong Signature in File Header")
	}
	return nil
}

func (file *File) Read(position, length int) ([]byte, error) {
	if _, err := file.inner.Seek(int64(position), io.SeekStart); err != nil {
		return nil, err
	}
	buffer := make([]byte, length)
	if n, err := file.inner.Read(buffer); err != nil {
		return nil, err
	} else if n != length {
		return nil, fmt.Errorf("Cannot Read (Position: %d, Length: %d)", position, length)
	}
	return buffer, nil
}

func (file *File) ReadTableListRootAddress() (int, error) {
	buffer, err := file.Read(
		FileHeaderTableListRootAddressPosition,
		FileHeaderTableListRootAddressLength)
	if err != nil {
		return 0, err
	}
	address := byteOrder.Uint32(buffer)
	return int(address), nil
}

func (seg *Segment) Buffer() []byte {
	return seg.buffer
}
