// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"

	"github.com/neetsdkasu/avltree"
	"github.com/neetsdkasu/avltree/intkey"
)

type IdleSegmentManager struct {
	tree *IdleSegmentTree
}

type IdleSegmentTree struct {
	file  *File
	cache map[int]*IdleSegmentTreeNode
}

type IdleSegmentTreeNode struct {
	tree              *IdleSegmentTree
	segment           *Segment
	key               avltree.Key
	leftChildAddress  int
	rightChildAddress int
	height            int
	updated           bool
}

func NewIdleSegmentTree(file *File) *IdleSegmentTree {
	tree := &IdleSegmentTree{
		file:  file,
		cache: make(map[int]*IdleSegmentTreeNode),
	}
	return tree
}

const (
	IdleSegmentTreeNodeLeftChildPosition = 0
	IdleSegmentTreeNodeLeftChildLength   = AddressSize

	IdleSegmentTreeNodeRightChildPosition = IdleSegmentTreeNodeLeftChildPosition + IdleSegmentTreeNodeLeftChildLength
	IdleSegmentTreeNodeRightChildLength   = AddressSize

	IdleSegmentTreeNodeHeightPosition = IdleSegmentTreeNodeRightChildPosition + IdleSegmentTreeNodeRightChildLength
	IdleSegmentTreeNodeHeightLength   = AddressSize
)

// avltree.NodeからIdleSegmentTreeNodeを取り出す
func unwrapIdleSegmentTreeNode(node avltree.Node) *IdleSegmentTreeNode {
	if node == nil {
		return nil
	}
	if n, ok := node.(*IdleSegmentTreeNode); ok {
		return n
	}
	logger.Panicf("[BUG] node is not IdleSegmentTreeNode (%#v)", node)
	return nil
}

// バグを見つけるためだけの処理
// IdleSegmentTreeはUnkoDBの外に公開して使うものではなくUnkoDB内部だけで完結するため
// 通常は直接変換 value.(*Segment) で取り出せばいいのだが
// UnkoDBが完成するまでは、念のための処理
// TODO 完成したら除去する
func unwrapIdleSegmentTreeValue(value any) *Segment {
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

// IdleSegmentTreeNodeをavltree.Nodeに変換する
func (node *IdleSegmentTreeNode) toNode() avltree.Node {
	if node == nil {
		return nil
	} else {
		return node
	}
}

// ノードのファイル上の位置を返す
func (node *IdleSegmentTreeNode) position() int {
	if node == nil {
		return NullAddress
	} else {
		return node.segment.Position()
	}
}

// ノードの変更をファイルに書き込む
func (node *IdleSegmentTreeNode) flush() error {
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
func (tree *IdleSegmentTree) loadNode(address int) *IdleSegmentTreeNode {
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
	node := &IdleSegmentTreeNode{
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
func (tree *IdleSegmentTree) Root() avltree.Node {
	node := tree.loadNode(tree.file.IdleSegmentTreeRootAddress())
	return node.toNode()
}

// github.com/neetsdkasu/avltree.RealTree.NewNode(...) の実装
func (tree *IdleSegmentTree) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value any) avltree.RealNode {
	node := &IdleSegmentTreeNode{
		tree:              tree,
		key:               key,
		segment:           unwrapIdleSegmentTreeValue(value),
		leftChildAddress:  unwrapIdleSegmentTreeNode(leftChild).position(),
		rightChildAddress: unwrapIdleSegmentTreeNode(rightChild).position(),
		height:            height,
		updated:           true,
	}
	tree.cache[node.position()] = node
	return node
}

// github.com/neetsdkasu/avltree.RealTree.SetRoot(...)の実装
func (tree *IdleSegmentTree) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	address := unwrapIdleSegmentTreeNode(newRoot).position()
	err := tree.file.UpdateIdleSegmentTreeRootAddress(address)
	if err != nil {
		logger.Panic(err)
	}
	return tree
}

// github.com/neetsdkasu/avltree.RealTree.AllowDuplicateKeys() の実装
func (*IdleSegmentTree) AllowDuplicateKeys() bool {
	return true
}

// github.com/neetsdkasu/avltree.RealNode.Key() の実装
func (node *IdleSegmentTreeNode) Key() avltree.Key {
	return intkey.IntKey(node.segment.BufferSize())
}

// github.com/neetsdkasu/avltree.RealNode.Value() の実装
func (node *IdleSegmentTreeNode) Value() any {
	return node.segment
}

// github.com/neetsdkasu/avltree.RealNode.LeftChild() の実装
func (node *IdleSegmentTreeNode) LeftChild() avltree.Node {
	leftChild := node.tree.loadNode(node.leftChildAddress)
	return leftChild.toNode()
}

// github.com/neetsdkasu/avltree.RealNode.RightChild() の実装
func (node *IdleSegmentTreeNode) RightChild() avltree.Node {
	rightChild := node.tree.loadNode(node.rightChildAddress)
	return rightChild.toNode()
}

// github.com/neetsdkasu/avltree.RealNode.SetValue(...) の実装
func (*IdleSegmentTreeNode) SetValue(newValue any) (_ avltree.Node) {
	// IdleSegmentでは値(Segment)を更新する状況はない
	logger.Panic("[BUG] Unreachable")
	return
}

// github.com/neetsdkasu/avltree.RealNode.Height() の実装
func (node *IdleSegmentTreeNode) Height() int {
	return node.height
}

// github.com/neetsdkasu/avltree.RealNode.SetChildren(...) の実装
func (node *IdleSegmentTreeNode) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	node.leftChildAddress = unwrapIdleSegmentTreeNode(newLeftChild).position()
	node.rightChildAddress = unwrapIdleSegmentTreeNode(newRightChild).position()
	node.height = newHeight
	node.updated = true
	return node
}

// github.com/neetsdkasu/avltree.RealNode.Set(...) の実装
func (*IdleSegmentTreeNode) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue any) (_ avltree.RealNode) {
	// IdleSegmentでは値(Segment)を更新する状況はない
	logger.Panic("[BUG] Unreachable")
	return
}
