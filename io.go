package unkodb

import "encoding/binary"

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
