// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"github.com/neetsdkasu/avltree"
)

type rootAddressGetter = func() (addr int, err error)
type rootAddressSetter = func(addr int) (err error)

type tableTree struct {
	segManager *segmentManager
	table      *Table
	cache      map[int]*tableTreeNode
}

type tableTreeNode struct {
	tree              *tableTree
	seg               *segmentBuffer
	leftChildAddress  int
	rightChildAddress int
	height            int
	updated           bool
}

type tableTreeValue = map[string]interface{}

func unwrapTableTreeNode(node avltree.Node) (_ *tableTreeNode) {
	if node == nil {
		return nil
	}
	if ttn, ok := node.(*tableTreeNode); ok {
		return ttn
	} else {
		bug.Panicf("unwrapTableTreeNode: unknown type %T %#v", node, node)
		return
	}
}

func (node *tableTreeNode) toNode() avltree.Node {
	if node == nil {
		return nil
	} else {
		return node
	}
}

func (node *tableTreeNode) position() int {
	if node == nil {
		return nullAddress
	} else {
		return node.seg.Position()
	}
}

func (node *tableTreeNode) writeValue(record tableTreeValue) {
	tree := node.tree
	buf := node.seg.Buffer()[tableTreeNodeHeaderByteSize:]
	w := newByteEncoder(newByteSliceWriter(buf), fileByteOrder)
	keyValue := record[tree.table.key.Name()]
	err := tree.table.key.write(w, keyValue)
	if err != nil {
		bug.Panicf("tableTreeNode.writeValue: key %#v %v", tree.table.key, err)
	}
	for _, col := range tree.table.columns {
		colValue := record[col.Name()]
		err = col.write(w, colValue)
		if err != nil {
			bug.Panicf("tableTreeNode.writeValue: column %#v %v", col, err)
		}
	}
}

func (tree *tableTree) calcSegmentByteSize(record tableTreeValue) int {
	var segmentByteSize = tableTreeNodeHeaderByteSize
	if keyValue, ok := record[tree.table.key.Name()]; !ok {
		bug.Panic("tableTree.calcSegmentByteSize: not found key value")
	} else {
		segmentByteSize += tree.table.key.byteSizeHint(keyValue)
	}
	for _, col := range tree.table.columns {
		if colValue, ok := record[col.Name()]; !ok {
			bug.Panicf("tableTree.calcSegmentByteSize: not found value of %s", col.Name())
		} else {
			segmentByteSize += col.byteSizeHint(colValue)
		}
	}
	return segmentByteSize
}

// github.com/neetsdkasu/avltree.RealTree.Root() の実装
func (tree *tableTree) Root() avltree.Node {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealTree.NewNode(...) の実装
func (tree *tableTree) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value any) avltree.RealNode {
	// leftChildAddress + rightChildAddress + height[1 byte]
	record, ok := value.(tableTreeValue)
	if !ok {
		bug.Panicf("tableTree.NewNode: invalid value %#v", value)
	}
	segmentByteSize := tree.calcSegmentByteSize(record)
	seg, err := tree.segManager.Request(segmentByteSize)
	if err != nil {
		panic(err) // ファイルIOエラー
	}
	node := &tableTreeNode{
		tree:              tree,
		seg:               seg,
		leftChildAddress:  unwrapTableTreeNode(leftChild).position(),
		rightChildAddress: unwrapTableTreeNode(rightChild).position(),
		height:            height,
		updated:           true,
	}
	node.writeValue(record)
	return node
}

// github.com/neetsdkasu/avltree.RealTree.SetRoot(...)の実装
func (tree *tableTree) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealTree.AllowDuplicateKeys() の実装
func (*tableTree) AllowDuplicateKeys() bool {
	return false
}

// github.com/neetsdkasu/avltree.RealNode.Key() の実装
func (node *tableTreeNode) Key() avltree.Key {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.Value() の実装
func (node *tableTreeNode) Value() any {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.LeftChild() の実装
func (node *tableTreeNode) LeftChild() avltree.Node {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.RightChild() の実装
func (node *tableTreeNode) RightChild() avltree.Node {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.SetValue(...) の実装
func (node *tableTreeNode) SetValue(newValue any) (_ avltree.Node) {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.Height() の実装
func (node *tableTreeNode) Height() int {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.SetChildren(...) の実装
func (node *tableTreeNode) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.Set(...) の実装
func (node *tableTreeNode) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue any) (_ avltree.RealNode) {
	panic("TODO")
}
