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
		entriesOffset:             headerSize,
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
	var buf [headerSize]byte
	header := bytes.NewBuffer(buf[:0])
	header.Write(signature)
	binary.Write(header, binary.BigEndian, uint16(headerSize))
	binary.Write(header, binary.BigEndian, uint16(FormatVersion))
	binary.Write(header, binary.BigEndian, int32(db.tableIdTableRootAddress))
	binary.Write(header, binary.BigEndian, int32(db.idleEntryTableRootAddress))
	binary.Write(header, binary.BigEndian, uint32(db.entriesTotalByteSize))
	_, err := db.file.Seek(db.fileOffset, io.SeekStart)
	if err != nil {
		return err
	}
	if headerSize != header.Len() {
		return fmt.Errorf("[BUG] WRONG HEADER SIZE: %d (expect %d)", header.Len(), headerSize)
	}
	_, err = header.WriteTo(db.file)
	return err
}

func (db *UnkoDB) readHeader() error {
	var sig [signatureAreaSize]byte
	_, err := db.file.Seek(db.fileOffset, io.SeekStart)
	if err != nil {
		return err
	}
	n, err := io.ReadFull(db.file, sig[:])
	if err != nil {
		return fmt.Errorf("FAILED TO READ SIGNATURE (%w)", err)
	}
	if signatureAreaSize != n {
		return fmt.Errorf("FAILED TO READ SIGNATURE")
	}
	if !bytes.Equal(signature, sig[:]) {
		return fmt.Errorf("INVALID SIGNATURE: %#v", sig[:])
	}
	var hSize uint16
	if err = binary.Read(db.file, binary.BigEndian, &hSize); err != nil {
		return fmt.Errorf("FAILED TO READ HEADER SIZE (%w)", err)
	}
	buf := make([]byte, int(hSize)-signatureAreaSize-headerSizeAreaSize)
	n, err = io.ReadFull(db.file, buf)
	if err != nil {
		return fmt.Errorf("FAILED TO READ HEADER (%w)", err)
	}
	if len(buf) != n {
		return fmt.Errorf("FAILED TO READ HEADER")
	}
	r := bytes.NewReader(buf)
	var fVer uint16
	if err = binary.Read(r, binary.BigEndian, &fVer); err != nil {
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
	var (
		tableIdTableRootAddress   int32
		idleEntryTableRootAddress int32
		entriesTotalByteSize      uint32
	)
	if err = binary.Read(r, binary.BigEndian, &tableIdTableRootAddress); err != nil {
		return fmt.Errorf("FAILED TO READ tableIdTableRootAddress (%w)", err)
	}
	if err = binary.Read(r, binary.BigEndian, &idleEntryTableRootAddress); err != nil {
		return fmt.Errorf("FAILED TO READ idleEntryTableRootAddress (%w)", err)
	}
	if err = binary.Read(r, binary.BigEndian, &entriesTotalByteSize); err != nil {
		return fmt.Errorf("FAILED TO READ entriesTotalByteSize (%w)", err)
	}
	db.entriesOffset = int64(hSize)
	db.tableIdTableRootAddress = int64(tableIdTableRootAddress)
	db.idleEntryTableRootAddress = int64(idleEntryTableRootAddress)
	db.entriesTotalByteSize = int64(entriesTotalByteSize)
	return nil
}
