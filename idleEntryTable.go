package unkodb

import (
	"github.com/neetsdkasu/avltree"
	. "github.com/neetsdkasu/avltree/intkey"
)

type idleEntryTable struct {
	db *UnkoDB
}

type idleEntry struct {
	table *idleEntryTable
	node  *nodeInfo
}

func (table *idleEntryTable) Add(size, address int) bool {
	_, ok := avltree.Insert(table, false, IntKey(size), address)
	// TODO SetRootが呼ばれているかもしれんので保存する必要あり・・・？
	return ok
}

func (*idleEntryTable) unwrap(node avltree.Node) int {
	if node == nil {
		return noAddress
	} else {
		return node.(*idleEntry).node.address
	}
}

func (table *idleEntryTable) Root() avltree.Node {
	address := int(table.db.idleEntryTableRootAddress)
	if noAddress == address {
		return nil
	}
	node, err := table.db.readUint32KeyNode(address)
	if err != nil {
		// TODO どうする・・・？
	}
	return &idleEntry{table, node}
}

func (table *idleEntryTable) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	table.db.idleEntryTableRootAddress = int64(table.unwrap(newRoot))
	return table
}

func (table *idleEntryTable) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value interface{}) avltree.RealNode {
	// TODO
	panic("NOT IMPLEMENT")
}

func (table *idleEntryTable) AllowDuplicateKeys() bool {
	return true
}

func (entry *idleEntry) Key() avltree.Key {
	panic("NOT IMPLEMENT")
}

func (entry *idleEntry) Value() interface{} {
	panic("NOT IMPLEMENT")
}

func (entry *idleEntry) LeftChild() avltree.Node {
	panic("NOT IMPLEMENT")
}

func (entry *idleEntry) RightChild() avltree.Node {
	panic("NOT IMPLEMENT")
}

func (entry *idleEntry) SetValue(newValue interface{}) avltree.Node {
	panic("NOT IMPLEMENT")
}

func (entry *idleEntry) Height() int {
	panic("NOT IMPLEMENT")
}

func (entry *idleEntry) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	panic("NOT IMPLEMENT")
}

func (entry *idleEntry) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue interface{}) avltree.RealNode {
	panic("NOT IMPLEMENT")
}
