package unkodb

import (
	"github.com/neetsdkasu/avltree"
	. "github.com/neetsdkasu/avltree/stringkey"
)

type tableIdTable struct{ db *UnkoDB }

type tableIdEntry struct {
	table *tableIdTable
	node  *nodeInfo
	key   StringKey
}

func (table *tableIdTable) Root() avltree.Node {
	panic("NOT IMPLEMENT")
}

func (table *tableIdTable) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	panic("NOT IMPLEMENT")
}

func (table *tableIdTable) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value interface{}) avltree.RealNode {
	panic("NOT IMPLEMENT")
}

func (table *tableIdTable) AllowDuplicateKeys() bool {
	return false
}

func (entyr *tableIdEntry) Key() avltree.Key {
	panic("NOT IMPLEMENT")
}

func (entyr *tableIdEntry) Value() interface{} {
	panic("NOT IMPLEMENT")
}

func (entry *tableIdEntry) LeftChild() avltree.Node {
	panic("NOT IMPLEMENT")
}

func (entry *tableIdEntry) RightChild() avltree.Node {
	panic("NOT IMPLEMENT")
}

func (entry *tableIdEntry) SetValue(newValue interface{}) avltree.Node {
	panic("NOT IMPLEMENT")
}

func (entry *tableIdEntry) Height() int {
	return entry.node.height
}

func (entry *tableIdEntry) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	panic("NOT IMPLEMENT")
}

func (entry *tableIdEntry) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue interface{}) avltree.RealNode {
	panic("NOT IMPLEMENT")
}
