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

// avltree.NodeからIdleSegmentListTreeNodeを取り出す
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

// バグを見つけるためだけの処理
// IdleSegmentListTreeはUnkoDBの外に公開して使うものではなくUnkoDB内部だけで完結するため
// 通常は直接変換 value.(*Segment) で取り出せばいいのだが
// UnkoDBが完成するまでは、念のための処理
// TODO 完成したら除去する
func unwrapIdleSegmentListTreeValue(value any) *Segment {
	if value == nil {
		logger.Panic("[BUG] value is nil")
	}
	if seg, ok := value.(*Segment); ok {
		if seg == nil {
			logger.Panic("[BUG] value is *Segment(nil)")
		}
		return seg
	}
	logger.Panicf("[BUG] value is not Segment (%#v)", value)
	return nil
}

// IdleSegmentListTreeNodeをavltree.Nodeに変換する
func (node *IdleSegmentListTreeNode) toNode() avltree.Node {
	if node == nil {
		return nil
	} else {
		return node
	}
}

// ノードのファイル上の位置を返す
func (node *IdleSegmentListTreeNode) position() int {
	if node == nil {
		return NullAddress
	} else {
		return node.segment.Position()
	}
}

// ノードの変更をファイルに書き込む
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
		return err // ファイルのIOエラー
	}
	node.updated = false
	return nil
}

// ノード情報をファイルから読み取る
func (tree *IdleSegmentListTree) loadNode(address int) *IdleSegmentListTreeNode {
	if address == NullAddress {
		return nil
	}
	if cached, ok := tree.cache[address]; ok {
		return cached
	}
	seg, err := tree.file.ReadSegment(address)
	if err != nil {
		logger.Panic(err) // ファイルのIOエラー
	}
	r := NewByteDecoder(bytes.NewReader(seg.Buffer()), fileByteOrder)
	var leftChildAddress int32
	err = r.Int32(&leftChildAddress)
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがあるか、不正なファイル
	}
	var rightChildAddress int32
	err = r.Int32(&rightChildAddress)
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがあるか、不正なファイル
	}
	var height int32
	err = r.Int32(&height)
	if err != nil {
		logger.Panic(err) // ここに到達したらどこかにバグがあるか、不正なファイル
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

// github.com/neetsdkasu/avltree.RealTree.Root() の実装
func (tree *IdleSegmentListTree) Root() avltree.Node {
	node := tree.loadNode(tree.file.IdleSegmentListRootAddress())
	return node.toNode()
}

// github.com/neetsdkasu/avltree.RealTree.NewNode(...) の実装
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

// github.com/neetsdkasu/avltree.RealTree.SetRoot(...)の実装
func (tree *IdleSegmentListTree) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	address := unwrapIdleSegmentListTreeNode(newRoot).position()
	err := tree.file.UpdateIdleSegmentListRootAddress(address)
	if err != nil {
		logger.Panic(err)
	}
	return tree
}

// github.com/neetsdkasu/avltree.RealTree.AllowDuplicateKeys() の実装
func (*IdleSegmentListTree) AllowDuplicateKeys() bool {
	return true
}

// github.com/neetsdkasu/avltree.RealNode.Key() の実装
func (node *IdleSegmentListTreeNode) Key() avltree.Key {
	return intkey.IntKey(node.segment.BufferSize())
}

// github.com/neetsdkasu/avltree.RealNode.Value() の実装
func (node *IdleSegmentListTreeNode) Value() any {
	return node.segment
}

// github.com/neetsdkasu/avltree.RealNode.LeftChild() の実装
func (node *IdleSegmentListTreeNode) LeftChild() avltree.Node {
	leftChild := node.tree.loadNode(node.leftChildAddress)
	return leftChild.toNode()
}

// github.com/neetsdkasu/avltree.RealNode.RightChild() の実装
func (node *IdleSegmentListTreeNode) RightChild() avltree.Node {
	rightChild := node.tree.loadNode(node.rightChildAddress)
	return rightChild.toNode()
}

// github.com/neetsdkasu/avltree.RealNode.SetValue(...) の実装
func (*IdleSegmentListTreeNode) SetValue(newValue any) (_ avltree.Node) {
	// IdleSegmentListでは値(Segment)を更新する状況はない
	logger.Panic("[BUG] Unreachable")
	return
}

// github.com/neetsdkasu/avltree.RealNode.Height() の実装
func (node *IdleSegmentListTreeNode) Height() int {
	return node.height
}

// github.com/neetsdkasu/avltree.RealNode.SetChildren(...) の実装
func (node *IdleSegmentListTreeNode) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	node.leftChildAddress = unwrapIdleSegmentListTreeNode(newLeftChild).position()
	node.rightChildAddress = unwrapIdleSegmentListTreeNode(newRightChild).position()
	node.height = newHeight
	node.updated = true
	return node
}

// github.com/neetsdkasu/avltree.RealNode.Set(...) の実装
func (*IdleSegmentListTreeNode) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue any) (_ avltree.RealNode) {
	// IdleSegmentListでは値(Segment)を更新する状況はない
	logger.Panic("[BUG] Unreachable")
	return
}
