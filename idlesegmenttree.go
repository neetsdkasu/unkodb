// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"

	"github.com/neetsdkasu/avltree"
)

type idleSegmentTreeNodeCache = map[int]*idleSegmentTreeNode

type idleSegmentTree struct {
	file        *fileAccessor
	rootAddress int
	updatedRoot bool
	cache       idleSegmentTreeNodeCache
}

type idleSegmentTreeNode struct {
	tree              *idleSegmentTree
	segment           *segmentBuffer
	key               avltree.Key
	leftChildAddress  int
	rightChildAddress int
	height            int
	updated           bool
}

var idleSegmentTreeKey = intKey[int32]

func newIdleSegmentTree(file *fileAccessor) *idleSegmentTree {
	tree := &idleSegmentTree{
		file:        file,
		rootAddress: file.IdleSegmentTreeRootAddress(),
		updatedRoot: false,
		cache:       make(idleSegmentTreeNodeCache),
	}
	return tree
}

// avltree.NodeからIdleSegmentTreeNodeを取り出す
func unwrapIdleSegmentTreeNode(node avltree.Node) *idleSegmentTreeNode {
	if node == nil {
		return nil
	}
	if n, ok := node.(*idleSegmentTreeNode); ok {
		return n
	}
	bug.Panicf("unwrapIdleSegmentTreeNode: node is not IdleSegmentTreeNode (%#v)", node)
	return nil
}

// バグを見つけるためだけの処理
// IdleSegmentTreeはUnkoDBの外に公開して使うものではなくUnkoDB内部だけで完結するため
// 通常は直接変換 value.(*Segment) で取り出せばいいのだが
// UnkoDBが完成するまでは、念のための処理
// TODO 完成したら除去する
func unwrapIdleSegmentTreeValue(value any) (_ *segmentBuffer) {
	if value == nil {
		bug.Panic("unwrapIdleSegmentTreeValue: value is nil")
	}
	if seg, ok := value.(*segmentBuffer); ok {
		if seg == nil {
			bug.Panic("unwrapIdleSegmentTreeValue: value is *Segment(nil)")
		}
		return seg
	}
	bug.Panicf("unwrapIdleSegmentTreeValue: value is not Segment (%#v)", value)
	return
}

// IdleSegmentTreeNodeをavltree.Nodeに変換する
func (node *idleSegmentTreeNode) toNode() avltree.Node {
	if node == nil {
		return nil
	} else {
		return node
	}
}

// ノードのファイル上の位置を返す
func (node *idleSegmentTreeNode) position() int {
	if node == nil {
		return nullAddress
	} else {
		return node.segment.Position()
	}
}

// ノードの変更をファイルに書き込む
func (node *idleSegmentTreeNode) flush() error {
	if node == nil || !node.updated {
		return nil
	}
	buf := node.segment.Buffer()
	w := newByteEncoder(newByteSliceWriter(buf), fileByteOrder)
	err := w.Int32(int32(node.leftChildAddress))
	if err != nil {
		bug.Panic(err) // ここに到達したらどこかにバグがある
	}
	err = w.Int32(int32(node.rightChildAddress))
	if err != nil {
		bug.Panic(err) // ここに到達したらどこかにバグがある
	}
	err = w.Uint8(uint8(node.height))
	if err != nil {
		bug.Panic(err) // ここに到達したらどこかにバグがある
	}
	err = node.segment.Flush()
	if err != nil {
		return err // ファイルのIOエラー
	}
	node.updated = false
	return nil
}

func (tree *idleSegmentTree) clearCache() {
	for _, node := range tree.cache {
		if node.updated {
			bug.Panic("idleSegmentTree.clearCache: not flush")
		}
	}
	// 定期的にキャッシュクリアする仕組みが欲しいのかも？
	// アクセスの古いノードからとか？わからん
	tree.cache = make(idleSegmentTreeNodeCache)
}

func (tree *idleSegmentTree) flush() error {
	for _, node := range tree.cache {
		err := node.flush()
		if err != nil {
			return err
		}
	}
	if tree.updatedRoot {
		err := tree.file.UpdateIdleSegmentTreeRootAddress(tree.rootAddress)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (tree *idleSegmentTree) getCache(address int) (*idleSegmentTreeNode, bool) {
	node, ok := tree.cache[address]
	return node, ok
}

func (tree *idleSegmentTree) addCache(node *idleSegmentTreeNode) {
	tree.cache[node.position()] = node
}

// ノード情報をファイルから読み取る
func (tree *idleSegmentTree) loadNode(address int) *idleSegmentTreeNode {
	if address == nullAddress {
		return nil
	}
	if cachedNode, ok := tree.getCache(address); ok {
		return cachedNode
	}
	seg, err := tree.file.ReadPartialSegment(address, idleSegmentTreeNodeDataByteSize)
	if err != nil {
		panic(err) // ファイルのIOエラー
	}
	r := newByteDecoder(bytes.NewReader(seg.Buffer()), fileByteOrder)
	var leftChildAddress int32
	err = r.Int32(&leftChildAddress)
	if err != nil {
		// TODO ちゃんと記述する
		panic(WrongFileFormat{err.Error()}) // 不正なファイル(segmentのサイズ情報が壊れている、など)
	}
	var rightChildAddress int32
	err = r.Int32(&rightChildAddress)
	if err != nil {
		// TODO ちゃんと記述する
		panic(WrongFileFormat{err.Error()}) // 不正なファイル(segmentのサイズ情報が壊れている、など)
	}
	var height uint8
	err = r.Uint8(&height)
	if err != nil {
		// TODO ちゃんと記述する
		panic(WrongFileFormat{err.Error()}) // 不正なファイル(segmentのサイズ情報が壊れている、など)
	}
	node := &idleSegmentTreeNode{
		tree:              tree,
		segment:           seg,
		key:               idleSegmentTreeKey(int32(seg.Size())),
		leftChildAddress:  int(leftChildAddress),
		rightChildAddress: int(rightChildAddress),
		height:            int(height),
		updated:           false,
	}
	tree.addCache(node)
	return node
}

// github.com/neetsdkasu/avltree.RealTree.Root() の実装
func (tree *idleSegmentTree) Root() avltree.Node {
	node := tree.loadNode(tree.rootAddress)
	return node.toNode()
}

// github.com/neetsdkasu/avltree.RealTree.NewNode(...) の実装
func (tree *idleSegmentTree) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value any) avltree.RealNode {
	node := &idleSegmentTreeNode{
		tree:              tree,
		key:               key,
		segment:           unwrapIdleSegmentTreeValue(value),
		leftChildAddress:  unwrapIdleSegmentTreeNode(leftChild).position(),
		rightChildAddress: unwrapIdleSegmentTreeNode(rightChild).position(),
		height:            height,
		updated:           true,
	}
	tree.addCache(node)
	return node
}

// github.com/neetsdkasu/avltree.RealTree.SetRoot(...)の実装
func (tree *idleSegmentTree) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	tree.rootAddress = unwrapIdleSegmentTreeNode(newRoot).position()
	tree.updatedRoot = true
	return tree
}

// github.com/neetsdkasu/avltree.RealTree.AllowDuplicateKeys() の実装
func (*idleSegmentTree) AllowDuplicateKeys() bool {
	return true
}

// github.com/neetsdkasu/avltree.RealNode.Key() の実装
func (node *idleSegmentTreeNode) Key() avltree.Key {
	return idleSegmentTreeKey(int32(node.segment.Size()))
}

// github.com/neetsdkasu/avltree.RealNode.Value() の実装
func (node *idleSegmentTreeNode) Value() any {
	return node.segment
}

// github.com/neetsdkasu/avltree.RealNode.LeftChild() の実装
func (node *idleSegmentTreeNode) LeftChild() avltree.Node {
	leftChild := node.tree.loadNode(node.leftChildAddress)
	return leftChild.toNode()
}

// github.com/neetsdkasu/avltree.RealNode.RightChild() の実装
func (node *idleSegmentTreeNode) RightChild() avltree.Node {
	rightChild := node.tree.loadNode(node.rightChildAddress)
	return rightChild.toNode()
}

// github.com/neetsdkasu/avltree.RealNode.SetValue(...) の実装
func (*idleSegmentTreeNode) SetValue(newValue any) (_ avltree.Node) {
	// IdleSegmentでは値(Segment)を更新する状況はない
	bug.Panic("idleSegmentTreeNode.SetValue: Unreachable")
	return
}

// github.com/neetsdkasu/avltree.RealNode.Height() の実装
func (node *idleSegmentTreeNode) Height() int {
	return node.height
}

// github.com/neetsdkasu/avltree.RealNode.SetChildren(...) の実装
func (node *idleSegmentTreeNode) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	node.leftChildAddress = unwrapIdleSegmentTreeNode(newLeftChild).position()
	node.rightChildAddress = unwrapIdleSegmentTreeNode(newRightChild).position()
	node.height = newHeight
	node.updated = true
	return node
}

// github.com/neetsdkasu/avltree.RealNode.Set(...) の実装
func (*idleSegmentTreeNode) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue any) (_ avltree.RealNode) {
	// IdleSegmentでは値(Segment)を更新する状況はない
	bug.Panic("idleSegmentTreeNode.Set: Unreachable")
	return
}
