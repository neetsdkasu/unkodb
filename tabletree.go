// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"github.com/neetsdkasu/avltree"
)

type tableTree struct {
	file  *fileAccessor
	table *Table
}

type tableTreeNode struct {
	tree *tableTree
}

// github.com/neetsdkasu/avltree.RealTree.Root() の実装
func (tree *tableTree) Root() avltree.Node {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealTree.NewNode(...) の実装
func (tree *tableTree) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value any) avltree.RealNode {
	panic("TODO")
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
