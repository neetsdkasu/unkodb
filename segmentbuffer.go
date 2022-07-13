// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"fmt"
)

type segmentBuffer struct {
	file        *fileAccessor
	position    int
	buffer      []byte
	segmentSize int
	partial     bool
}

func (seg *segmentBuffer) Position() int {
	return seg.position
}

func (seg *segmentBuffer) Size() int {
	return seg.segmentSize - segmentHeaderByteSize
}

// 不要かも (どこからも呼び出されてないはず)
func (seg *segmentBuffer) IsPartial() bool {
	return seg.partial
}

func (seg *segmentBuffer) Buffer() []byte {
	return seg.buffer[segmentHeaderByteSize:]
}

func (seg *segmentBuffer) Clear() {
	fillBytes(seg.Buffer(), 0)
}

func (seg *segmentBuffer) Flush() error {
	err := seg.file.Write(seg.position, seg.buffer)
	if err != nil {
		return fmt.Errorf("Failed segmentBuffer.Flush [%w]", err)
	}
	return nil
}

func (seg *segmentBuffer) LoadFullSegment() error {
	if seg.partial {
		buffer, err := seg.file.ReadBytes(seg.position, seg.segmentSize)
		if err != nil {
			return err
		}
		seg.buffer = buffer
		seg.partial = false
	}
	return nil
}
