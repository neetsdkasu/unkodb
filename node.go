package unkodb

import "io"

type nodeInfo struct {
	address    int
	size       int
	height     int
	leftChild  int
	rightChild int
}

func (db *UnkoDB) seekNodeHead(address int) (err error) {
	_, err = db.file.Seek(db.entriesOffset+int64(address), io.SeekStart)
	return
}

func (db *UnkoDB) writeNodeInfo(node *nodeInfo) error {
	if err := db.seekNodeHead(node.address); err != nil {
		return err
	}
	if err := db.writeUint16(uint16(node.size)); err != nil {
		return err
	}
	if err := db.writeUint16(uint16(node.height)); err != nil {
		return err
	}
	if err := db.writeInt32(int32(node.leftChild)); err != nil {
		return err
	}
	if err := db.writeInt32(int32(node.rightChild)); err != nil {
		return err
	}
	return nil
}

func (db *UnkoDB) readNodeInfo(address int) (*nodeInfo, error) {
	if err := db.seekNodeHead(address); err != nil {
		return nil, err
	}
	size, err := db.readUint16()
	if err != nil {
		return nil, err
	}
	height, err := db.readUint16()
	if err != nil {
		return nil, err
	}
	leftChild, err := db.readInt32()
	if err != nil {
		return nil, err
	}
	rightChild, err := db.readInt32()
	if err != nil {
		return nil, err
	}
	node := &nodeInfo{
		address:    address,
		size:       int(size),
		height:     int(height),
		leftChild:  int(leftChild),
		rightChild: int(rightChild),
	}
	return node, nil
}
