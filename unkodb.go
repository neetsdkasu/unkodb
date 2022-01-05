package unkodb

import (
	"bytes"
	"fmt"
	"io"
)

const dbFormatVersion = 1

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

const (
	limitEntriesTotalByteSize = 0x7FFF_FFFF - headerSize
	limitEntryByteSize        = 0x8000
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
	if _, err := db.file.Seek(db.fileOffset, io.SeekStart); err != nil {
		return err
	}
	if err := db.writeBytes(signature); err != nil {
		return fmt.Errorf("FAILED TO WRITE SIGNATURE (%w)", err)
	}
	if err := db.writeUint16(headerSize); err != nil {
		return fmt.Errorf("FAILED TO WRITE HEADER SIZE VALUE (%w)", err)
	}
	if err := db.writeUint16(dbFormatVersion); err != nil {
		return fmt.Errorf("FAILED TO WRITE FORMAT VERSION (%w)", err)
	}
	if err := db.writeInt32(int32(db.tableIdTableRootAddress)); err != nil {
		return fmt.Errorf("FAILED TO WRITE tableIdTableRootAddress (%w)", err)
	}
	if err := db.writeInt32(int32(db.idleEntryTableRootAddress)); err != nil {
		return fmt.Errorf("FAILED TO WRITE idleEntryTableRootAddress (%w)", err)
	}
	if err := db.writeUint32(uint32(db.entriesTotalByteSize)); err != nil {
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
	if dbFormatVersion != fVer {
		// まだ dbFormatVersion=1だから・・・
		return fmt.Errorf("INFALID FORMAT VERSION: %d", fVer)
	}
	if headerSize != hSize {
		// まだ dbFormatVersion=1だからheaderSizeに変化はない・・・
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
	db.entriesOffset = db.fileOffset + int64(hSize)
	db.tableIdTableRootAddress = int64(tableIdTableRootAddress)
	db.idleEntryTableRootAddress = int64(idleEntryTableRootAddress)
	db.entriesTotalByteSize = int64(entriesTotalByteSize)
	return nil
}

func (db *UnkoDB) newEmptyEntry(size int) (address, realSize int, err error) {
	if size <= 0 || limitEntryByteSize < size {
		err = fmt.Errorf("EXCEED LIMIT ENTRY SIZE: %d (must be 0 < size <= %d)", size, limitEntryByteSize)
		return
	}
	realSize = ((size + 3) >> 2) << 2
	table := idleEntryTable{db}
	var ok bool
	address, ok, err = table.take(realSize)
	if ok || err != nil {
		return
	}
	if limitEntriesTotalByteSize < db.entriesTotalByteSize+int64(realSize) {
		err = fmt.Errorf("EXCEED LIMIT DB-ENTRIES SIZE: %d", db.entriesTotalByteSize+int64(realSize))
		return
	}
	address64 := db.entriesOffset + db.entriesTotalByteSize
	if _, err = db.file.Seek(address64, io.SeekStart); err != nil {
		return
	}
	if err = db.writeUint16(uint16(realSize)); err != nil {
		return
	}
	if err = db.writeBytes(make([]byte, realSize-2)); err != nil {
		return
	}
	address = int(address64 - db.entriesOffset)
	db.entriesTotalByteSize += int64(realSize)
	return
}
