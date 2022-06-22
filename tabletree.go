// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"

	"github.com/neetsdkasu/avltree"
)

type rootAddressAccessor interface {
	getRootAddress() (addr int, err error)
	setRootAddress(addr int) (err error)
}

type tableTreeNodeCache = map[int]*tableTreeNode

type tableTree struct {
	table       *Table
	segManager  *segmentManager
	rootAddress int
	updatedRoot bool
	cache       tableTreeNodeCache
}

type tableTreeNode struct {
	tree              *tableTree
	seg               *segmentBuffer
	key               avltree.Key
	leftChildAddress  int
	rightChildAddress int
	height            int
	updated           bool
}

type tableTreeValue = map[string]interface{}

func newTableTree(table *Table, segManager *segmentManager) (*tableTree, error) {
	rootAddress, err := table.rootAccessor.getRootAddress()
	if err != nil {
		return nil, err
	}
	tree := &tableTree{
		table:       table,
		segManager:  segManager,
		rootAddress: rootAddress,
		updatedRoot: false,
		cache:       make(tableTreeNodeCache),
	}
	return tree, nil
}

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

func (tree *tableTree) flush() (err error) {
	for _, node := range tree.cache {
		err = node.flush()
		if err != nil {
			return
		}
	}
	if tree.updatedRoot {
		err = tree.table.rootAccessor.setRootAddress(tree.rootAddress)
		if err != nil {
			return
		}
	}
	return
}

func (node *tableTreeNode) flush() (err error) {
	if node == nil || !node.updated {
		return
	}
	buf := node.seg.Buffer()[:tableTreeNodeHeaderByteSize]
	w := newByteEncoder(newByteSliceWriter(buf), fileByteOrder)
	err = w.Int32(int32(node.leftChildAddress))
	if err != nil {
		bug.Panic(err)
	}
	err = w.Int32(int32(node.rightChildAddress))
	if err != nil {
		bug.Panic(err)
	}
	err = w.Uint8(uint8(node.height))
	if err != nil {
		bug.Panic(err)
	}
	return node.seg.Flush()
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

func (tree *tableTree) clearCache() {
	for _, node := range tree.cache {
		if node.updated {
			bug.Panic("tableTree.clearCache: not flush")
		}
	}
	// 定期的にキャッシュクリアする仕組みが欲しいのかも？
	// アクセスの古いノードからとか？わからん
	tree.cache = make(tableTreeNodeCache)
}

func (tree *tableTree) getCache(addr int) (node *tableTreeNode, ok bool) {
	node, ok = tree.cache[addr]
	return
}

func (tree *tableTree) addCache(node *tableTreeNode) {
	tree.cache[node.position()] = node
}

func (tree *tableTree) loadNode(addr int) *tableTreeNode {
	if addr == nullAddress {
		return nil
	}
	if cachedNode, ok := tree.getCache(addr); ok {
		return cachedNode
	}
	seg, err := tree.segManager.LoadSegment(addr)
	if err != nil {
		panic(err) // たぶんファイルIOエラー、バグの場合もあるかも
	}
	r := newByteDecoder(bytes.NewReader(seg.Buffer()), fileByteOrder)
	var leftChildAddress int32
	err = r.Int32(&leftChildAddress)
	if err != nil {
		bug.Panic(err) // ここに到達したらどこかにバグがあるか
		// panic(WrongFileFormat) // 不正なファイル(segmentのサイズ情報が壊れている、など)
	}
	var rightChildAddress int32
	err = r.Int32(&rightChildAddress)
	if err != nil {
		panic(WrongFileFormat) // 不正なファイル(segmentのサイズ情報が壊れている、など)
	}
	var height uint8
	err = r.Uint8(&height)
	if err != nil {
		panic(WrongFileFormat) // 不正なファイル(segmentのサイズ情報が壊れている、など)
	}
	keyValue, err := tree.table.key.read(r)
	if err != nil {
		panic(WrongFileFormat) // 不正なファイル(segmentのサイズ情報が壊れている、など)
	}
	node := &tableTreeNode{
		tree:              tree,
		seg:               seg,
		key:               tree.table.key.toKey(keyValue),
		leftChildAddress:  int(leftChildAddress),
		rightChildAddress: int(rightChildAddress),
		height:            int(height),
		updated:           false,
	}
	tree.addCache(node)
	return node
}

// github.com/neetsdkasu/avltree.RealTree.Root() の実装
func (tree *tableTree) Root() avltree.Node {
	return tree.loadNode(tree.rootAddress).toNode()
}

// github.com/neetsdkasu/avltree.RealTree.NewNode(...) の実装
func (tree *tableTree) NewNode(leftChild, rightChild avltree.Node, height int, key avltree.Key, value any) avltree.RealNode {
	// leftChildAddress + rightChildAddress + height[1 byte]
	record, ok := value.(tableTreeValue)
	if !ok {
		bug.Panicf("tableTree.NewNode: invalid value %#v", value)
	}
	segmentByteSize := tree.calcSegmentByteSize(record)
	seg, err := tree.segManager.EmptySegment(segmentByteSize)
	if err != nil {
		panic(err) // ファイルIOエラー
	}
	if keyValue, ok := record[tree.table.key.Name()]; !ok {
		bug.Panic("tableTree.NewNode: no key")
	} else if key.CompareTo(tree.table.key.toKey(keyValue)) != avltree.EqualToOtherKey {
		bug.Panicf("tableTree.NewNode: not mutch key %v %v", key, record)
	}
	node := &tableTreeNode{
		tree:              tree,
		seg:               seg,
		key:               key,
		leftChildAddress:  unwrapTableTreeNode(leftChild).position(),
		rightChildAddress: unwrapTableTreeNode(rightChild).position(),
		height:            height,
		updated:           true,
	}
	node.writeValue(record)
	tree.addCache(node)
	return node
}

// github.com/neetsdkasu/avltree.RealTree.SetRoot(...)の実装
func (tree *tableTree) SetRoot(newRoot avltree.RealNode) avltree.RealTree {
	tree.rootAddress = unwrapTableTreeNode(newRoot).position()
	tree.updatedRoot = true
	return tree
}

// github.com/neetsdkasu/avltree.RealTree.AllowDuplicateKeys() の実装
func (*tableTree) AllowDuplicateKeys() bool {
	return false
}

// github.com/neetsdkasu/avltree.RealNode.Key() の実装
func (node *tableTreeNode) Key() avltree.Key {
	return node.key
}

// github.com/neetsdkasu/avltree.RealNode.Value() の実装
func (node *tableTreeNode) Value() any {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.LeftChild() の実装
func (node *tableTreeNode) LeftChild() avltree.Node {
	return node.tree.loadNode(node.leftChildAddress).toNode()
}

// github.com/neetsdkasu/avltree.RealNode.RightChild() の実装
func (node *tableTreeNode) RightChild() avltree.Node {
	return node.tree.loadNode(node.rightChildAddress).toNode()
}

// github.com/neetsdkasu/avltree.RealNode.SetValue(...) の実装
func (node *tableTreeNode) SetValue(newValue any) (_ avltree.Node) {
	panic("TODO")
}

// github.com/neetsdkasu/avltree.RealNode.Height() の実装
func (node *tableTreeNode) Height() int {
	return node.height
}

// github.com/neetsdkasu/avltree.RealNode.SetChildren(...) の実装
func (node *tableTreeNode) SetChildren(newLeftChild, newRightChild avltree.Node, newHeight int) avltree.RealNode {
	node.leftChildAddress = unwrapTableTreeNode(newLeftChild).position()
	node.rightChildAddress = unwrapTableTreeNode(newRightChild).position()
	node.height = newHeight
	node.updated = true
	return node
}

// github.com/neetsdkasu/avltree.RealNode.Set(...) の実装
func (node *tableTreeNode) Set(newLeftChild, newRightChild avltree.Node, newHeight int, newValue any) (_ avltree.RealNode) {
	node.SetChildren(newLeftChild, newRightChild, newHeight)
	node.SetValue(newValue)
	return node
}
