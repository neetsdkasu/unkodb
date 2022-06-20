// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"github.com/neetsdkasu/avltree"
)

type segmentManager struct {
	file *fileAccessor
	tree *idleSegmentTree
}

func newSegmentManager(file *fileAccessor) *segmentManager {
	manager := &segmentManager{
		file: file,
		tree: newIdleSegmentTree(file),
	}
	return manager
}

func (manager *segmentManager) Request(byteSize int) (_ *segmentBuffer, err error) {
	defer catchError(&err)
	keyMin := idleSegmentTreeKey(int32(byteSize))
	keyMax := idleSegmentTreeKey(int32(byteSize + 4))
	_, nodes := avltree.DeleteRangeIterate(manager.tree, false, keyMin, keyMax, func(key avltree.Key, value any) (deleteNode, breakIteration bool) {
		deleteNode = true
		breakIteration = true
		return
	})
	if len(nodes) == 0 {
		return manager.file.CreateSegment(byteSize)
	}
	if len(nodes) != 1 {
		panicf("[BUG] free segment too many %d", len(nodes))
	}
	seg, ok := nodes[0].Value().(*segmentBuffer)
	if !ok {
		panicf("[BUG] not segmentBuffer %T %#v", nodes[0], nodes[0])
	}
	return seg, nil
}

func (manager *segmentManager) Release(seg *segmentBuffer) (err error) {
	defer catchError(&err)
	key := idleSegmentTreeKey(int32(seg.BufferSize()))
	_, ok := avltree.Insert(manager.tree, false, key, seg)
	if !ok {
		panic("[BUG] cann not insert free segment")
	}
	return
}
