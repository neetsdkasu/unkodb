// unkodb
// author: Leonardone @ NEETSDKASU

// ファイルヘッダフォーマット
//  シグネチャ
///   16 byte
//      3 5 7 11 13 17 19 23 29 31 'U' 'N' 'K' 'O' 'D' 'B'
//  フォーマットバージョン番号 (1から始める、255行くことはないと思うが一応2byte確保)
//    2 byte (uint16)
//  予備領域（後で追加で情報を置きたくなったときの情報を置く場所のメモリ位置（アドレス？）を入れる）
//    4 byte (int32)
//  テーブル一覧のルートノードを示すメモリ位置（アドレス？） (0の場合はテーブルなし)
//    4 byte (int32)
//  空き領域断片のルートノードを示すメモリ位置（アドレス？） (0の場合は断片なし)
//    4 byte (int32)

package unkodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var fileByteOrder = binary.BigEndian

const (
	FileFormatVersion = 1

	AddressSize = 4
	NullAddress = 0

	FileHeaderSignaturePosition = 0
	FileHeaderSignatureLength   = 16

	FileHeaderFileFormatVersionPosition = FileHeaderSignaturePosition + FileHeaderSignatureLength
	FileHeaderFileFormatVersionLength   = 2

	FileHeaderNextNewSegmentAddressPosition = FileHeaderFileFormatVersionPosition + FileHeaderFileFormatVersionLength
	FileHeaderNextNewSegmentAddressLength   = AddressSize

	FileHeaderReserveAreaAddressPosition = FileHeaderNextNewSegmentAddressPosition + FileHeaderNextNewSegmentAddressLength
	FileHeaderReserveAreaAddressLength   = AddressSize

	FileHeaderTableListRootAddressPosition = FileHeaderReserveAreaAddressPosition + FileHeaderReserveAreaAddressLength
	FileHeaderTableListRootAddressLength   = AddressSize

	FileHeaderIdleSegmentTreeRootAddressPosition = FileHeaderTableListRootAddressPosition + FileHeaderTableListRootAddressLength
	FileHeaderIdleSegmentTreeRootAddressLength   = AddressSize

	FileHeaderSize = FileHeaderIdleSegmentTreeRootAddressPosition + FileHeaderIdleSegmentTreeRootAddressLength

	FirstNewSegmentAddress = FileHeaderSize

	SegmentHeaderSize = AddressSize
)

type File struct {
	inner                      io.ReadWriteSeeker
	version                    int
	nextNewSegmentAddress      int
	tableListRootAddress       int
	idleSegmentListRootAddress int
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

func ReadFile(file io.ReadWriteSeeker) (*File, error) {
	fileSize, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("Failed File.ReadFile [%w]", err)
	}
	newFile := &File{
		inner:                      file,
		version:                    0,
		nextNewSegmentAddress:      NullAddress,
		tableListRootAddress:       NullAddress,
		idleSegmentListRootAddress: NullAddress,
	}
	if fileSize < FileHeaderSize {
		return nil, fmt.Errorf("Wrong File Format")
	}
	if err = newFile.readHeader(); err != nil {
		return nil, err
	}
	return newFile, nil
}

func InitializeFile(file io.ReadWriteSeeker) (*File, error) {
	newFile := &File{
		inner:                      file,
		version:                    FileFormatVersion,
		nextNewSegmentAddress:      FirstNewSegmentAddress,
		tableListRootAddress:       NullAddress,
		idleSegmentListRootAddress: NullAddress,
	}
	var buffer [FileHeaderSize]byte
	w := NewByteEncoder(NewByteSliceWriter(buffer[:]), fileByteOrder)
	if err := w.RawBytes(Signature()); err != nil {
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Uint16(FileFormatVersion); err != nil {
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Int32(FirstNewSegmentAddress); err != nil {
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Int32(NullAddress); err != nil {
		// ReserveAreaAddress
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Int32(NullAddress); err != nil {
		// TableListRootAddress
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Int32(NullAddress); err != nil {
		// IdleSegmentTreeRootAddress
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := newFile.Write(0, buffer[:]); err != nil {
		return nil, fmt.Errorf("Failed write reservearea [%w]", err)
	}
	return newFile, nil
}

func (file *File) readHeader() error {
	var buffer [FileHeaderSize]byte
	if err := file.Read(0, buffer[:]); err != nil {
		return err
	}
	r := NewByteDecoder(bytes.NewReader(buffer[:]), fileByteOrder)
	{
		var sig [FileHeaderSignatureLength]byte
		if err := r.RawBytes(sig[:]); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if !bytes.Equal(sig[:], Signature()) {
			return fmt.Errorf("Wrong Signature in File Header")
		}
	}
	{
		var version uint16
		if err := r.Uint16(&version); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if version != FileFormatVersion {
			return fmt.Errorf("Unsupported FileFormatVersion (%d)", version)
		}
		file.version = int(version)
	}
	{
		var nextNewSegmentAddress int32
		if err := r.Int32(&nextNewSegmentAddress); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if nextNewSegmentAddress < FirstNewSegmentAddress {
			return fmt.Errorf("Wrong NextNewSegmentAddress")
		}
		file.nextNewSegmentAddress = int(nextNewSegmentAddress)
	}
	{
		var reserveAreaAddress int32
		if err := r.Int32(&reserveAreaAddress); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if reserveAreaAddress != NullAddress {
			return fmt.Errorf("Wrong ReserveAreaAddress")
		}
	}
	{
		var tableListRootAddress int32
		if err := r.Int32(&tableListRootAddress); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if tableListRootAddress < 0 {
			return fmt.Errorf("Wrong TableListRootAddress")
		}
		file.tableListRootAddress = int(tableListRootAddress)
	}
	{
		var idleSegmentListRootAddress int32
		if err := r.Int32(&idleSegmentListRootAddress); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if idleSegmentListRootAddress < 0 {
			return fmt.Errorf("Wrong IdleSegmentTreeRootAddress")
		}
		file.idleSegmentListRootAddress = int(idleSegmentListRootAddress)
	}
	return nil
}

func (file *File) Read(position int, buffer []byte) error {
	if _, err := file.inner.Seek(int64(position), io.SeekStart); err != nil {
		return fmt.Errorf("Failed File.Read (seek) [%w]", err)
	}
	if n, err := io.ReadFull(file.inner, buffer); err != nil {
		return fmt.Errorf("Failed File.Read (read) [%w]", err)
	} else if n != len(buffer) {
		return fmt.Errorf("Failed File.Read [cannot read (Position: %d, Length: %d, Read: %d)]", position, len(buffer), n)
	}
	return nil
}

func (file *File) ReadBytes(position, length int) ([]byte, error) {
	buffer := make([]byte, length)
	err := file.Read(position, buffer)
	if err != nil {
		return nil, fmt.Errorf("Failed File.ReadBytes [%w]", err)
	}
	return buffer, nil
}

func (file *File) Write(position int, data []byte) error {
	if _, err := file.inner.Seek(int64(position), io.SeekStart); err != nil {
		return fmt.Errorf("Failed File.Write (seek) [%w]", err)
	}
	if n, err := file.inner.Write(data); err != nil {
		return fmt.Errorf("Failed File.Write (write) [%w]", err)
	} else if n != len(data) {
		return fmt.Errorf("Failed File.Write [cannot write (Position: %d, Data Length: %d, Wrote: %d)]", position, len(data), n)
	}
	return nil
}

func (file *File) CreateSegment(length int) (*Segment, error) {
	segmentAddress := file.nextNewSegmentAddress
	_, err := file.inner.Seek(int64(segmentAddress), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("Failed File.CreateSegment (seek) [%w]", err)
	}
	length += SegmentHeaderSize
	buffer := make([]byte, length)
	err = NewByteEncoder(NewByteSliceWriter(buffer), fileByteOrder).Int32(int32(length))
	if err != nil {
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	err = file.Write(segmentAddress, buffer)
	if err != nil {
		return nil, fmt.Errorf("Failed File.CreateSegment (write) [%w]", err)
	}
	nextNewSegmentAddress := segmentAddress + length
	err = file.UpdateNextNewSegmentAddress(nextNewSegmentAddress)
	if err != nil {
		return nil, fmt.Errorf("Failed File.CreateSegment (update) [%w]", err)
	}
	seg := &Segment{
		file:     file,
		position: segmentAddress,
		buffer:   buffer,
	}
	return seg, nil
}

func (file *File) ReadSegment(position int) (*Segment, error) {
	var headerBuffer [SegmentHeaderSize]byte
	err := file.Read(position, headerBuffer[:])
	if err != nil {
		return nil, fmt.Errorf("Failed File.ReadSegment (read header) [%w]", err)
	}
	length := int(fileByteOrder.Uint32(headerBuffer[:]))
	buffer, err := file.ReadBytes(position, length)
	if err != nil {
		return nil, fmt.Errorf("Failed File.ReadSegment (read data) [%w]", err)
	}
	seg := &Segment{file, position, buffer}
	return seg, nil
}

func (file *File) NextNewSegmentAddress() int {
	return file.nextNewSegmentAddress
}

func (file *File) UpdateNextNewSegmentAddress(newAddress int) error {
	var buffer [FileHeaderNextNewSegmentAddressLength]byte
	fileByteOrder.PutUint32(buffer[:], uint32(newAddress))
	err := file.Write(FileHeaderNextNewSegmentAddressPosition, buffer[:])
	if err != nil {
		return fmt.Errorf("Failed File.UpdateNextNewSegmentAddress [%w]", err)
	}
	file.nextNewSegmentAddress = newAddress
	return nil
}

func (file *File) TableListRootAddress() int {
	return file.tableListRootAddress
}

func (file *File) UpdateTableListRootAddress(newAddress int) error {
	var buffer [FileHeaderTableListRootAddressLength]byte
	fileByteOrder.PutUint32(buffer[:], uint32(newAddress))
	err := file.Write(FileHeaderTableListRootAddressPosition, buffer[:])
	if err != nil {
		return fmt.Errorf("Failed File.UpdateTableListRootAddress [%w]", err)
	}
	file.tableListRootAddress = newAddress
	return nil
}

func (file *File) IdleSegmentTreeRootAddress() int {
	return file.idleSegmentListRootAddress
}

func (file *File) UpdateIdleSegmentTreeRootAddress(newAddress int) error {
	var buffer [FileHeaderIdleSegmentTreeRootAddressLength]byte
	fileByteOrder.PutUint32(buffer[:], uint32(newAddress))
	err := file.Write(FileHeaderIdleSegmentTreeRootAddressPosition, buffer[:])
	if err != nil {
		return fmt.Errorf("Failed File.UpdateIdleSegmentTreeRootAddress [%w]", err)
	}
	file.idleSegmentListRootAddress = newAddress
	return nil
}

func (seg *Segment) Position() int {
	return seg.position
}

func (seg *Segment) BufferSize() int {
	return len(seg.buffer) - SegmentHeaderSize
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
