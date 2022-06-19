// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

type tableTree struct {
	file *fileAccessor
	spec *Table
}

type tableTreeNode struct {
	tree *tableTree
}
