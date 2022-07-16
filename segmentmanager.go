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

func (manager *segmentManager) LoadSegment(addr int) (*segmentBuffer, error) {
	return manager.file.ReadSegment(addr)
}

func (manager *segmentManager) LoadPartialSegment(addr int, size int) (*segmentBuffer, error) {
	return manager.file.ReadPartialSegment(addr, size)
}

func (manager *segmentManager) EmptySegment(byteSize uint64) (*segmentBuffer, error) {
	byteSize = (byteSize + 3) &^ 3
	if byteSize < minimumSegmentByteSize {
		bug.Panic("too small")
	}
	if byteSize > maximumSegmentByteSize {
		return nil, TooLargeData
	}
	keyMin := idleSegmentTreeKey(int32(byteSize))
	keyMax := idleSegmentTreeKey(int32(minValue(byteSize+32, maximumSegmentByteSize)))
	_, nodes := avltree.DeleteRangeIterate(manager.tree, false, keyMin, keyMax, func(key avltree.Key, value any) (deleteNode, breakIteration bool) {
		deleteNode = true
		breakIteration = true
		return
	})
	if len(nodes) == 0 {
		mul2Size := byteSize*2 + segmentHeaderByteSize
		mul3Size := byteSize*3 + segmentHeaderByteSize
		if mul2Size > maximumSegmentByteSize {
			return manager.file.CreateSegment(int(byteSize))
		}
		mul3Size = minValue(mul3Size, maximumSegmentByteSize)
		keyMin := idleSegmentTreeKey(int32(mul2Size))
		keyMax := idleSegmentTreeKey(int32(mul3Size))
		_, nodes := avltree.DeleteRangeIterate(manager.tree, false, keyMin, keyMax, func(key avltree.Key, value any) (deleteNode, breakIteration bool) {
			deleteNode = true
			breakIteration = true
			return
		})
		if len(nodes) == 0 {
			return manager.file.CreateSegment(int(byteSize))
		}
		if len(nodes) != 1 {
			bug.Panicf("segmentManager.Request: free segment too many %d", len(nodes))
		}
		seg, ok := nodes[0].Value().(*segmentBuffer)
		if !ok {
			bug.Panicf("segmentManager.Request: not segmentBuffer %T %#v", nodes[0], nodes[0])
		}
		other, err := seg.Split(int(byteSize))
		if err != nil {
			// どこからも参照のない迷子セグメントになる・・・？
			return nil, err
		}
		err = manager.ReleaseSegment(other)
		if err != nil {
			// どこからも参照のない迷子セグメントになる・・・？
			return nil, err
		}
		return seg, nil
	}
	if len(nodes) != 1 {
		bug.Panicf("segmentManager.Request: free segment too many %d", len(nodes))
	}
	seg, ok := nodes[0].Value().(*segmentBuffer)
	if !ok {
		bug.Panicf("segmentManager.Request: not segmentBuffer %T %#v", nodes[0], nodes[0])
	}
	err := seg.LoadFullSegment()
	if err != nil {
		return nil, err
	}
	err = manager.tree.flush()
	if err != nil {
		return nil, err
	}
	manager.tree.clearCache()
	return seg, nil
}

func (manager *segmentManager) ReleaseSegmentByAddress(segmentAddress int) error {
	seg, err := manager.file.ReadPartialSegment(segmentAddress, idleSegmentTreeNodeDataByteSize)
	if err != nil {
		return err
	}
	key := idleSegmentTreeKey(int32(seg.Size()))
	_, ok := avltree.Insert(manager.tree, false, key, seg)
	if !ok {
		bug.Panic("segmentManager.Release: cann not insert free segment")
	}
	err = manager.tree.flush()
	if err != nil {
		return err
	}
	manager.tree.clearCache()
	return nil
}

func (manager *segmentManager) ReleaseSegment(seg *segmentBuffer) error {
	if len(seg.Buffer()) < idleSegmentTreeNodeDataByteSize {
		bug.Panic("segmentManager.Release: invalid segment size")
	}
	key := idleSegmentTreeKey(int32(seg.Size()))
	_, ok := avltree.Insert(manager.tree, false, key, seg)
	if !ok {
		bug.Panic("segmentManager.Release: cann not insert free segment")
	}
	err := manager.tree.flush()
	if err != nil {
		return err
	}
	manager.tree.clearCache()
	return nil
}
