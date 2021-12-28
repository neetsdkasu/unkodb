package unkodb

import (
	"github.com/neetsdkasu/avltree"
	. "github.com/neetsdkasu/avltree/intkey"
)

type idleEntryTable struct{ db *UnkoDB }

type idleEntry struct {
	table *idleEntryTable
	node  *nodeInfo
	key   IntKey
}

func (table *idleEntryTable) add(size, address int) (ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()
	oldRoot := table.db.idleEntryTableRootAddress
	_, ok = avltree.Insert(table, false, IntKey(size), address)
	// TODO SetRootが呼ばれているかもしれんので保存する必要あり・・・？
	if oldRoot != table.db.idleEntryTableRootAddress {
		err = table.db.writeHeader()
	}
	return
}

func (*idleEntryTable) unwrap(node avltree.Node) int {
	if node == nil {
		return noAddress
	} else {
		return node.(*idleEntry).node.address
	}
}

func (table *idleEntryTable) readEntry(address int) avltree.Node {
	if noAddress == address {
		return nil
	}
	node, err := table.db.readNodeInfo(address)
	if err != nil {
		panic(err)
	}
	key, err := table.db.readUint32()
	if err != nil {
		panic(err)
	}
	return &idleEntry{table, node, IntKey(int(key))}
}

func (table *idleEntryTable) Root() avltree.Node {
	return table.readEntry(int(table.db.idleEntryTableRootAddress))
}

func (table *idleEntryTable) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	table.db.idleEntryTableRootAddress = int64(table.unwrap(newRoot))
	return table
}

func (table *idleEntryTable) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value interface{}) avltree.RealNode {
	realKey := key.(IntKey)
	node := &nodeInfo{
		address:    value.(int),
		size:       int(realKey),
		height:     height,
		leftChild:  table.unwrap(leftChild),
		rightChild: table.unwrap(rightChild),
	}
	if err := table.db.writeNodeInfo(node); err != nil {
		panic(err)
	}
	if err := table.db.writeUint32(uint32(int(realKey))); err != nil {
		panic(err)
	}
	return &idleEntry{table, node, realKey}
}

func (table *idleEntryTable) AllowDuplicateKeys() bool {
	return true
}

func (entry *idleEntry) Key() avltree.Key {
	return entry.key
}

func (entry *idleEntry) Value() interface{} {
	return entry.node.address
}

func (entry *idleEntry) LeftChild() avltree.Node {
	return entry.table.readEntry(entry.node.leftChild)
}

func (entry *idleEntry) RightChild() avltree.Node {
	return entry.table.readEntry(entry.node.rightChild)
}

func (entry *idleEntry) SetValue(newValue interface{}) avltree.Node {
	// 使い方的に呼ばれないはずだが・・・？
	panic("idleEntry.SetValue")
	// return entry
}

func (entry *idleEntry) Height() int {
	return entry.node.height
}

func (entry *idleEntry) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	// 回転での連続書き込み、ヤバそう
	entry.node.height = newHeight
	entry.node.leftChild = entry.table.unwrap(newLeftChild)
	entry.node.rightChild = entry.table.unwrap(newRightChild)
	if err := entry.table.db.writeNodeInfo(entry.node); err != nil {
		panic(err)
	}
	return entry
}

func (entry *idleEntry) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue interface{}) avltree.RealNode {
	// 使い方的に呼ばれないはずだが・・・？
	panic("idleEntry.Set")
	// return entry.SetChildren(newLeftChild, newRightChild, newHeight)
}
