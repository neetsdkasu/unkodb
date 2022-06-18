// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

type segmentManager struct {
	tree *idleSegmentTree
}

func newSegmentManager(file *fileAccessor) *segmentManager {
	manager := &segmentManager{
		tree: newIdleSegmentTree(file),
	}
	return manager
}
