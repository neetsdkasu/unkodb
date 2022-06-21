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

func (manager *segmentManager) Request(byteSize int) (*segmentBuffer, error) {
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
		bug.Panicf("segmentManager.Request: free segment too many %d", len(nodes))
	}
	seg, ok := nodes[0].Value().(*segmentBuffer)
	if !ok {
		bug.Panicf("segmentManager.Request: not segmentBuffer %T %#v", nodes[0], nodes[0])
	}
	return seg, nil
}

func (manager *segmentManager) Release(seg *segmentBuffer) {
	key := idleSegmentTreeKey(int32(seg.BufferSize()))
	_, ok := avltree.Insert(manager.tree, false, key, seg)
	if !ok {
		bug.Panic("segmentManager.Release: cann not insert free segment")
	}
}
