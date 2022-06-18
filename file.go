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
	fileFormatVersion = 1

	addressByteSize = 4
	nullAddress     = 0

	fileHeaderSignaturePosition = 0
	fileHeaderSignatureLength   = 16

	fileHeaderFileFormatVersionPosition = fileHeaderSignaturePosition + fileHeaderSignatureLength
	fileHeaderFileFormatVersionLength   = 2

	fileHeaderNextNewSegmentAddressPosition = fileHeaderFileFormatVersionPosition + fileHeaderFileFormatVersionLength
	fileHeaderNextNewSegmentAddressLength   = addressByteSize

	fileHeaderReserveAreaAddressPosition = fileHeaderNextNewSegmentAddressPosition + fileHeaderNextNewSegmentAddressLength
	fileHeaderReserveAreaAddressLength   = addressByteSize

	fileHeaderTableListRootAddressPosition = fileHeaderReserveAreaAddressPosition + fileHeaderReserveAreaAddressLength
	fileHeaderTableListRootAddressLength   = addressByteSize

	fileHeaderIdleSegmentTreeRootAddressPosition = fileHeaderTableListRootAddressPosition + fileHeaderTableListRootAddressLength
	fileHeaderIdleSegmentTreeRootAddressLength   = addressByteSize

	fileHeaderByteSize = fileHeaderIdleSegmentTreeRootAddressPosition + fileHeaderIdleSegmentTreeRootAddressLength

	firstNewSegmentAddress = fileHeaderByteSize

	segmentHeaderByteSize = addressByteSize
)

type fileAccessor struct {
	inner                      io.ReadWriteSeeker
	version                    int
	nextNewSegmentAddress      int
	tableListRootAddress       int
	idleSegmentListRootAddress int
}

type segmentBuffer struct {
	file     *fileAccessor
	position int
	buffer   []byte
}

func fileSignature() []byte {
	return []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
	}
}

func readFile(file io.ReadWriteSeeker) (*fileAccessor, error) {
	fileSize, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("Failed File.ReadFile [%w]", err)
	}
	newFile := &fileAccessor{
		inner:                      file,
		version:                    0,
		nextNewSegmentAddress:      nullAddress,
		tableListRootAddress:       nullAddress,
		idleSegmentListRootAddress: nullAddress,
	}
	if fileSize < fileHeaderByteSize {
		return nil, fmt.Errorf("Wrong File Format")
	}
	if err = newFile.readHeader(); err != nil {
		return nil, err
	}
	return newFile, nil
}

func initializeFile(file io.ReadWriteSeeker) (*fileAccessor, error) {
	newFile := &fileAccessor{
		inner:                      file,
		version:                    fileFormatVersion,
		nextNewSegmentAddress:      firstNewSegmentAddress,
		tableListRootAddress:       nullAddress,
		idleSegmentListRootAddress: nullAddress,
	}
	var buffer [fileHeaderByteSize]byte
	w := NewByteEncoder(NewByteSliceWriter(buffer[:]), fileByteOrder)
	if err := w.RawBytes(fileSignature()); err != nil {
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Uint16(fileFormatVersion); err != nil {
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Int32(firstNewSegmentAddress); err != nil {
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Int32(nullAddress); err != nil {
		// ReserveAreaAddress
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Int32(nullAddress); err != nil {
		// TableListRootAddress
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := w.Int32(nullAddress); err != nil {
		// IdleSegmentTreeRootAddress
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	if err := newFile.Write(0, buffer[:]); err != nil {
		return nil, fmt.Errorf("Failed write reservearea [%w]", err)
	}
	return newFile, nil
}

func (file *fileAccessor) readHeader() error {
	var buffer [fileHeaderByteSize]byte
	if err := file.Read(0, buffer[:]); err != nil {
		return err
	}
	r := NewByteDecoder(bytes.NewReader(buffer[:]), fileByteOrder)
	{
		var sig [fileHeaderSignatureLength]byte
		if err := r.RawBytes(sig[:]); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if !bytes.Equal(sig[:], fileSignature()) {
			return fmt.Errorf("Wrong Signature in File Header")
		}
	}
	{
		var version uint16
		if err := r.Uint16(&version); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if version != fileFormatVersion {
			return fmt.Errorf("Unsupported FileFormatVersion (%d)", version)
		}
		file.version = int(version)
	}
	{
		var nextNewSegmentAddress int32
		if err := r.Int32(&nextNewSegmentAddress); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if nextNewSegmentAddress < firstNewSegmentAddress {
			return fmt.Errorf("Wrong NextNewSegmentAddress")
		}
		file.nextNewSegmentAddress = int(nextNewSegmentAddress)
	}
	{
		var reserveAreaAddress int32
		if err := r.Int32(&reserveAreaAddress); err != nil {
			logger.Panic(err) // ここに到達する場合はバグがある
		}
		if reserveAreaAddress != nullAddress {
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

func (file *fileAccessor) Read(position int, buffer []byte) error {
	if _, err := file.inner.Seek(int64(position), io.SeekStart); err != nil {
		return fmt.Errorf("Failed fileAccessor.Read (seek) [%w]", err)
	}
	if n, err := io.ReadFull(file.inner, buffer); err != nil {
		return fmt.Errorf("Failed fileAccessor.Read (read) [%w]", err)
	} else if n != len(buffer) {
		return fmt.Errorf("Failed fileAccessor.Read [cannot read (Position: %d, Length: %d, Read: %d)]", position, len(buffer), n)
	}
	return nil
}

func (file *fileAccessor) ReadBytes(position, length int) ([]byte, error) {
	buffer := make([]byte, length)
	err := file.Read(position, buffer)
	if err != nil {
		return nil, fmt.Errorf("Failed fileAccessor.ReadBytes [%w]", err)
	}
	return buffer, nil
}

func (file *fileAccessor) Write(position int, data []byte) error {
	if _, err := file.inner.Seek(int64(position), io.SeekStart); err != nil {
		return fmt.Errorf("Failed fileAccessor.Write (seek) [%w]", err)
	}
	if n, err := file.inner.Write(data); err != nil {
		return fmt.Errorf("Failed fileAccessor.Write (write) [%w]", err)
	} else if n != len(data) {
		return fmt.Errorf("Failed fileAccessor.Write [cannot write (Position: %d, Data Length: %d, Wrote: %d)]", position, len(data), n)
	}
	return nil
}

func (file *fileAccessor) CreateSegment(length int) (*segmentBuffer, error) {
	segmentAddress := file.nextNewSegmentAddress
	_, err := file.inner.Seek(int64(segmentAddress), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("Failed fileAccessor.CreateSegment (seek) [%w]", err)
	}
	length += segmentHeaderByteSize
	buffer := make([]byte, length)
	err = NewByteEncoder(NewByteSliceWriter(buffer), fileByteOrder).Int32(int32(length))
	if err != nil {
		logger.Panic(err) // ここに到達する場合はバグがある
	}
	err = file.Write(segmentAddress, buffer)
	if err != nil {
		return nil, fmt.Errorf("Failed fileAccessor.CreateSegment (write) [%w]", err)
	}
	nextNewSegmentAddress := segmentAddress + length
	err = file.UpdateNextNewSegmentAddress(nextNewSegmentAddress)
	if err != nil {
		return nil, fmt.Errorf("Failed fileAccessor.CreateSegment (update) [%w]", err)
	}
	seg := &segmentBuffer{
		file:     file,
		position: segmentAddress,
		buffer:   buffer,
	}
	return seg, nil
}

func (file *fileAccessor) ReadSegment(position int) (*segmentBuffer, error) {
	var headerBuffer [segmentHeaderByteSize]byte
	err := file.Read(position, headerBuffer[:])
	if err != nil {
		return nil, fmt.Errorf("Failed fileAccessor.ReadSegment (read header) [%w]", err)
	}
	length := int(fileByteOrder.Uint32(headerBuffer[:]))
	buffer, err := file.ReadBytes(position, length)
	if err != nil {
		return nil, fmt.Errorf("Failed fileAccessor.ReadSegment (read data) [%w]", err)
	}
	seg := &segmentBuffer{file, position, buffer}
	return seg, nil
}

func (file *fileAccessor) NextNewSegmentAddress() int {
	return file.nextNewSegmentAddress
}

func (file *fileAccessor) UpdateNextNewSegmentAddress(newAddress int) error {
	var buffer [fileHeaderNextNewSegmentAddressLength]byte
	fileByteOrder.PutUint32(buffer[:], uint32(newAddress))
	err := file.Write(fileHeaderNextNewSegmentAddressPosition, buffer[:])
	if err != nil {
		return fmt.Errorf("Failed fileAccessor.UpdateNextNewSegmentAddress [%w]", err)
	}
	file.nextNewSegmentAddress = newAddress
	return nil
}

func (file *fileAccessor) TableListRootAddress() int {
	return file.tableListRootAddress
}

func (file *fileAccessor) UpdateTableListRootAddress(newAddress int) error {
	var buffer [fileHeaderTableListRootAddressLength]byte
	fileByteOrder.PutUint32(buffer[:], uint32(newAddress))
	err := file.Write(fileHeaderTableListRootAddressPosition, buffer[:])
	if err != nil {
		return fmt.Errorf("Failed fileAccessor.UpdateTableListRootAddress [%w]", err)
	}
	file.tableListRootAddress = newAddress
	return nil
}

func (file *fileAccessor) IdleSegmentTreeRootAddress() int {
	return file.idleSegmentListRootAddress
}

func (file *fileAccessor) UpdateIdleSegmentTreeRootAddress(newAddress int) error {
	var buffer [fileHeaderIdleSegmentTreeRootAddressLength]byte
	fileByteOrder.PutUint32(buffer[:], uint32(newAddress))
	err := file.Write(fileHeaderIdleSegmentTreeRootAddressPosition, buffer[:])
	if err != nil {
		return fmt.Errorf("Failed fileAccessor.UpdateIdleSegmentTreeRootAddress [%w]", err)
	}
	file.idleSegmentListRootAddress = newAddress
	return nil
}

func (seg *segmentBuffer) Position() int {
	return seg.position
}

func (seg *segmentBuffer) BufferSize() int {
	return len(seg.buffer) - segmentHeaderByteSize
}

func (seg *segmentBuffer) Buffer() []byte {
	return seg.buffer[segmentHeaderByteSize:]
}

func (seg *segmentBuffer) Flush() error {
	err := seg.file.Write(seg.position, seg.buffer)
	if err != nil {
		return fmt.Errorf("Failed segmentBuffer.Flush [%w]", err)
	}
	return nil
}
