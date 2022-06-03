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
}

const (
	IdleSegmentListTreeNodeLeftChildPosition = 0
	IdleSegmentListTreeNodeLeftChildLength   = AddressSize

	IdleSegmentListTreeNodeRightChildPosition = IdleSegmentListTreeNodeLeftChildPosition + IdleSegmentListTreeNodeLeftChildLength
	IdleSegmentListTreeNodeRightChildLength   = AddressSize

	IdleSegmentListTreeNodeHeightPosition = IdleSegmentListTreeNodeRightChildPosition + IdleSegmentListTreeNodeRightChildLength
	IdleSegmentListTreeNodeHeightLength   = AddressSize
)

func unwrapIdleSegmentListTreeNode(node any) *IdleSegmentListTreeNode {
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

func (node *IdleSegmentListTreeNode) position() int {
	if node == nil {
		return NullAddress
	} else {
		return node.segment.Position()
	}
}

func (node *IdleSegmentListTreeNode) loadNodeProperties() {
	r := NewByteDecoder(bytes.NewReader(node.segment.Buffer()), fileByteOrder)
	var leftChildAddress int32
	err := r.Int32(&leftChildAddress)
	if err != nil {
		logger.Panic(err)
	}
	var rightChildAddress int32
	err = r.Int32(&rightChildAddress)
	if err != nil {
		logger.Panic(err)
	}
	var height int32
	err = r.Int32(&height)
	if err != nil {
		logger.Panic(err)
	}
	node.leftChildAddress = int(leftChildAddress)
	node.rightChildAddress = int(rightChildAddress)
	node.height = int(height)
}

func (tree *IdleSegmentListTree) Root() avltree.Node {
	address := tree.file.IdleSegmentListRootAddress()
	if address == NullAddress {
		return nil
	}
	seg, err := tree.file.ReadSegment(address)
	if err != nil {
		logger.Panic(err)
	}
	node := &IdleSegmentListTreeNode{
		tree:              tree,
		segment:           seg,
		key:               intkey.IntKey(seg.BufferSize()),
		leftChildAddress:  NullAddress,
		rightChildAddress: NullAddress,
		height:            0,
	}
	node.loadNodeProperties()
	return node
}

func (tree *IdleSegmentListTree) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value any) avltree.RealNode {
	node := &IdleSegmentListTreeNode{
		tree:              tree,
		key:               key,
		segment:           unwrapIdleSegmentListTreeValue(value),
		leftChildAddress:  NullAddress,
		rightChildAddress: NullAddress,
		height:            height,
	}
	node.SetChildren(leftChild, rightChild, height)
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
	panic("TODO")
}

func (node *IdleSegmentListTreeNode) RightChild() avltree.Node {
	panic("TODO")
}

func (*IdleSegmentListTreeNode) SetValue(newValue any) (_ avltree.Node) {
	logger.Panic("[BUG] Unreachable")
	return
}

func (node *IdleSegmentListTreeNode) Height() int {
	panic("TODO")
}

func (node *IdleSegmentListTreeNode) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	panic("TODO")
}

func (*IdleSegmentListTreeNode) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue any) (_ avltree.RealNode) {
	// 値を設定する状況はないハズ
	logger.Panic("[BUG] Unreachable")
	return
}
