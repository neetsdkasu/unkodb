// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"
	"github.com/neetsdkasu/avltree"
	"github.com/neetsdkasu/avltree/intkey"
)

type IdleSegmentList struct {
	tree *IdleSegmentListTree
}

type IdleSegmentListTree struct {
	file  *File
	cache map[int]*IdleSegmentListTreeNode
}

type IdleSegmentListTreeNode struct {
	tree              *IdleSegmentListTree
	segment           *Segment
	key               avltree.Key
	leftChildAddress  int
	rightChildAddress int
	height            int
	updated           bool
}

func NewIdleSegmentListTree(file *File) *IdleSegmentListTree {
	tree := &IdleSegmentListTree{
		file:  file,
		cache: make(map[int]*IdleSegmentListTreeNode),
	}
	return tree
}

const (
	IdleSegmentListTreeNodeLeftChildPosition = 0
	IdleSegmentListTreeNodeLeftChildLength   = AddressSize

	IdleSegmentListTreeNodeRightChildPosition = IdleSegmentListTreeNodeLeftChildPosition + IdleSegmentListTreeNodeLeftChildLength
	IdleSegmentListTreeNodeRightChildLength   = AddressSize

	IdleSegmentListTreeNodeHeightPosition = IdleSegmentListTreeNodeRightChildPosition + IdleSegmentListTreeNodeRightChildLength
	IdleSegmentListTreeNodeHeightLength   = AddressSize
)

func unwrapIdleSegmentListTreeNode(node avltree.Node) *IdleSegmentListTreeNode {
	if node == nil {
		return nil
	}
	if n, ok := node.(*IdleSegmentListTreeNode); ok {
		return n
	}
	logger.Panicf("[BUG] node is not IdleSegmentListTreeNode (%#v)", node)
	return nil
}

func unwrapIdleSegmentListTreeValue(value any) *Segment {
	if value == nil {
		logger.Panic("[BUG] value is nil")
	}
	if seg, ok := value.(*Segment); ok {
		return seg
	}
	logger.Panicf("[BUG] value is not Segment (%#v)", value)
	return nil
}

func (node *IdleSegmentListTreeNode) toNode() avltree.Node {
	if node == nil {
		return nil
	} else {
		return node
	}
}

func (node *IdleSegmentListTreeNode) position() int {
	if node == nil {
		return NullAddress
	} else {
		return node.segment.Position()
	}
}

func (node *IdleSegmentListTreeNode) flush() error {
	if node == nil || !node.updated {
		return nil
	}
	buf := node.segment.Buffer()
	w := NewByteEncoder(NewByteSliceWriter(buf), fileByteOrder)
	err := w.Int32(int32(node.leftChildAddress))
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがある
	}
	err = w.Int32(int32(node.rightChildAddress))
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがある
	}
	err = w.Int32(int32(node.height))
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがある
	}
	err = node.segment.Flush()
	if err != nil {
		logger.Panic(err)
	}
	node.updated = false
	return nil
}

func (tree *IdleSegmentListTree) loadNode(address int) *IdleSegmentListTreeNode {
	if address == NullAddress {
		return nil
	}
	if cached, ok := tree.cache[address]; ok {
		return cached
	}
	seg, err := tree.file.ReadSegment(address)
	if err != nil {
		logger.Panic(err)
	}
	r := NewByteDecoder(bytes.NewReader(seg.Buffer()), fileByteOrder)
	var leftChildAddress int32
	err = r.Int32(&leftChildAddress)
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがある
	}
	var rightChildAddress int32
	err = r.Int32(&rightChildAddress)
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがある
	}
	var height int32
	err = r.Int32(&height)
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがある
	}
	node := &IdleSegmentListTreeNode{
		tree:              tree,
		segment:           seg,
		key:               intkey.IntKey(seg.BufferSize()),
		leftChildAddress:  int(leftChildAddress),
		rightChildAddress: int(rightChildAddress),
		height:            int(height),
		updated:           false,
	}
	tree.cache[address] = node
	return node
}

func (tree *IdleSegmentListTree) Root() avltree.Node {
	node := tree.loadNode(tree.file.IdleSegmentListRootAddress())
	return node.toNode()
}

func (tree *IdleSegmentListTree) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value any) avltree.RealNode {
	node := &IdleSegmentListTreeNode{
		tree:              tree,
		key:               key,
		segment:           unwrapIdleSegmentListTreeValue(value),
		leftChildAddress:  unwrapIdleSegmentListTreeNode(leftChild).position(),
		rightChildAddress: unwrapIdleSegmentListTreeNode(rightChild).position(),
		height:            height,
		updated:           true,
	}
	tree.cache[node.position()] = node
	return node
}

func (tree *IdleSegmentListTree) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	address := unwrapIdleSegmentListTreeNode(newRoot).position()
	err := tree.file.UpdateIdleSegmentListRootAddress(address)
	if err != nil {
		logger.Panic(err)
	}
	return tree
}

func (*IdleSegmentListTree) AllowDuplicateKeys() bool {
	return true
}

func (node *IdleSegmentListTreeNode) Key() avltree.Key {
	return intkey.IntKey(node.segment.BufferSize())
}

func (node *IdleSegmentListTreeNode) Value() any {
	return node.segment
}

func (node *IdleSegmentListTreeNode) LeftChild() avltree.Node {
	leftChild := node.tree.loadNode(node.leftChildAddress)
	return leftChild.toNode()
}

func (node *IdleSegmentListTreeNode) RightChild() avltree.Node {
	rightChild := node.tree.loadNode(node.rightChildAddress)
	return rightChild.toNode()
}

func (*IdleSegmentListTreeNode) SetValue(newValue any) (_ avltree.Node) {
	// IdleSegmentListでは値(Segment)を更新する状況はない
	logger.Panic("[BUG] Unreachable")
	return
}

func (node *IdleSegmentListTreeNode) Height() int {
	return node.height
}

func (node *IdleSegmentListTreeNode) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	node.leftChildAddress = unwrapIdleSegmentListTreeNode(newLeftChild).position()
	node.rightChildAddress = unwrapIdleSegmentListTreeNode(newRightChild).position()
	node.height = newHeight
	node.updated = true
	return node
}

func (*IdleSegmentListTreeNode) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue any) (_ avltree.RealNode) {
	// IdleSegmentListでは値(Segment)を更新する状況はない
	logger.Panic("[BUG] Unreachable")
	return
}
