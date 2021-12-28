package unkodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const FormatVersion = 1

var signature = []byte{
	0, 0, 0, 0, 0,
	'U', 'N', 'K', 'O', 'D', 'B',
	0, 0, 0, 0, 0,
}

// こういう埋め込みなんか怖い
const (
	signatureAreaSize                 = 16
	headerSizeAreaSize                = 2
	formatVersionAreaSize             = 2
	tableIdTableRootAddressAreaSize   = 4
	idleEntryTableRootAddressAreaSize = 4
	entriesTotalByteSizeAreaSize      = 4

	headerSize = signatureAreaSize +
		headerSizeAreaSize +
		formatVersionAreaSize +
		tableIdTableRootAddressAreaSize +
		idleEntryTableRootAddressAreaSize +
		entriesTotalByteSizeAreaSize
)

const noAddress = -1

type UnkoDB struct {
	file                      io.ReadWriteSeeker
	fileOffset                int64
	entriesOffset             int64
	tableIdTableRootAddress   int64
	idleEntryTableRootAddress int64
	entriesTotalByteSize      int64
	tables                    []Table
}

type Table interface{}

// ファイル内の現在位置からUnkoDBフォーマットで書き込む
func Create(file io.ReadWriteSeeker) (*UnkoDB, error) {
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	db := &UnkoDB{
		file:                      file,
		fileOffset:                offset,
		entriesOffset:             offset + headerSize,
		tables:                    nil,
		tableIdTableRootAddress:   noAddress,
		idleEntryTableRootAddress: noAddress,
		entriesTotalByteSize:      0,
	}
	if err = db.writeHeader(); err != nil {
		return nil, err
	}
	return db, nil
}

// ファイル内の現在位置からUnkoDBのフォーマットが始まっているとみなす
func Open(file io.ReadWriteSeeker) (*UnkoDB, error) {
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	db := &UnkoDB{
		file:       file,
		fileOffset: offset,
	}
	if err = db.readHeader(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *UnkoDB) writeHeader() error {
	_, err := db.file.Seek(db.fileOffset, io.SeekStart)
	if err != nil {
		return err
	}
	if err = db.writeBytes(signature); err != nil {
		return fmt.Errorf("FAILED TO WRITE SIGNATURE (%w)", err)
	}
	if err = db.writeUint16(headerSize); err != nil {
		return fmt.Errorf("FAILED TO WRITE HEADER SIZE VALUE (%w)", err)
	}
	if err = db.writeUint16(FormatVersion); err != nil {
		return fmt.Errorf("FAILED TO WRITE FORMAT VERSION (%w)", err)
	}
	if err = db.writeInt32(int32(db.tableIdTableRootAddress)); err != nil {
		return fmt.Errorf("FAILED TO WRITE tableIdTableRootAddress (%w)", err)
	}
	if err = db.writeInt32(int32(db.idleEntryTableRootAddress)); err != nil {
		return fmt.Errorf("FAILED TO WRITE idleEntryTableRootAddress (%w)", err)
	}
	if err = db.writeUint32(uint32(db.entriesTotalByteSize)); err != nil {
		return fmt.Errorf("FAILED TO WRITE entriesTotalByteSize (%w)", err)
	}
	return nil
}

func (db *UnkoDB) readHeader() error {
	var sig [signatureAreaSize]byte
	if _, err := db.file.Seek(db.fileOffset, io.SeekStart); err != nil {
		return err
	}
	if err := db.readBytes(sig[:]); err != nil {
		return fmt.Errorf("FAILED TO READ SIGNATURE (%w)", err)
	}
	if !bytes.Equal(signature, sig[:]) {
		return fmt.Errorf("INVALID SIGNATURE: %#v", sig[:])
	}
	hSize, err := db.readUint16()
	if err != nil {
		return fmt.Errorf("FAILED TO READ HEADER SIZE (%w)", err)
	}
	fVer, err := db.readUint16()
	if err != nil {
		return fmt.Errorf("FAILED TO READ FORMAT VERSION (%w)", err)
	}
	if FormatVersion != fVer {
		// まだ FormatVersion=1だから・・・
		return fmt.Errorf("INFALID FORMAT VERSION: %d", fVer)
	}
	if headerSize != hSize {
		// まだ FormatVersion=1だからheaderSizeに変化はない・・・
		return fmt.Errorf("INFALID HEADER SIZE VALUE: %d", hSize)
	}
	tableIdTableRootAddress, err := db.readInt32()
	if err != nil {
		return fmt.Errorf("FAILED TO READ tableIdTableRootAddress (%w)", err)
	}
	idleEntryTableRootAddress, err := db.readInt32()
	if err != nil {
		return fmt.Errorf("FAILED TO READ idleEntryTableRootAddress (%w)", err)
	}
	entriesTotalByteSize, err := db.readUint32()
	if err != nil {
		return fmt.Errorf("FAILED TO READ entriesTotalByteSize (%w)", err)
	}
	db.entriesOffset = int64(hSize)
	db.tableIdTableRootAddress = int64(tableIdTableRootAddress)
	db.idleEntryTableRootAddress = int64(idleEntryTableRootAddress)
	db.entriesTotalByteSize = int64(entriesTotalByteSize)
	return nil
}

func (db *UnkoDB) writeBytes(b []byte) error {
	return binary.Write(db.file, binary.BigEndian, b)
}

func (db *UnkoDB) readBytes(b []byte) error {
	return binary.Read(db.file, binary.BigEndian, b)
}

func (db *UnkoDB) writeUint16(v uint16) error {
	return binary.Write(db.file, binary.BigEndian, v)
}

func (db *UnkoDB) readUint16() (v uint16, err error) {
	err = binary.Read(db.file, binary.BigEndian, &v)
	return
}

func (db *UnkoDB) writeInt32(v int32) error {
	return binary.Write(db.file, binary.BigEndian, v)
}

func (db *UnkoDB) readInt32() (v int32, err error) {
	err = binary.Read(db.file, binary.BigEndian, &v)
	return
}

func (db *UnkoDB) writeUint32(v uint32) error {
	return binary.Write(db.file, binary.BigEndian, v)
}

func (db *UnkoDB) readUint32() (v uint32, err error) {
	err = binary.Read(db.file, binary.BigEndian, &v)
	return
}
