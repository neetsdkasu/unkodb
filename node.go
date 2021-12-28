package unkodb

import (
	"encoding/binary"
	"io"
)

const entryHeightOffset = 2

type nodeInfo struct {
	address    int
	leftChild  int
	rightChild int
	height     int
	key        interface{}
}

func (db *UnkoDB) writeNodeInfo(node *nodeInfo) error {
	if _, err := db.file.Seek(db.entriesOffset+int64(node.address)+entryHeightOffset, io.SeekStart); err != nil {
		return err
	}
	if err := binary.Write(db.file, binary.BigEndian, uint16(node.height)); err != nil {
		return err
	}
	if err := binary.Write(db.file, binary.BigEndian, int32(node.leftChild)); err != nil {
		return err
	}
	if err := binary.Write(db.file, binary.BigEndian, int32(node.rightChild)); err != nil {
		return err
	}
	return nil
}

func (db *UnkoDB) readNodeInfo(address int) (*nodeInfo, error) {
	if _, err := db.file.Seek(db.entriesOffset+int64(address)+entryHeightOffset, io.SeekStart); err != nil {
		return nil, err
	}
	var height uint16
	if err := binary.Read(db.file, binary.BigEndian, &height); err != nil {
		return nil, err
	}
	var leftChild int32
	if err := binary.Read(db.file, binary.BigEndian, &leftChild); err != nil {
		return nil, err
	}
	var rightChild int32
	if err := binary.Read(db.file, binary.BigEndian, &rightChild); err != nil {
		return nil, err
	}
	node := &nodeInfo{
		address:    address,
		leftChild:  int(leftChild),
		rightChild: int(rightChild),
		height:     int(height),
		key:        nil,
	}
	return node, nil
}

func (db *UnkoDB) readUint32KeyNode(address int) (*nodeInfo, error) {
	node, err := db.readNodeInfo(address)
	if err != nil {
		return nil, err
	}
	var key uint32
	if err = binary.Read(db.file, binary.BigEndian, &key); err != nil {
		return nil, err
	}
	node.key = key
	return node, nil
}
