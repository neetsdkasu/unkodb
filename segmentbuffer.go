// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"fmt"
)

type segmentBuffer struct {
	file     *fileAccessor
	position int
	buffer   []byte
}

func (seg *segmentBuffer) Position() int {
	return seg.position
}

func (seg *segmentBuffer) BufferSize() int {
	return len(seg.buffer) - segmentHeaderByteSize
}

func (seg *segmentBuffer) Buffer() []byte {
	return seg.buffer[segmentHeaderByteSize:]
}

func (seg *segmentBuffer) Flush() error {
	err := seg.file.Write(seg.position, seg.buffer)
	if err != nil {
		return fmt.Errorf("Failed segmentBuffer.Flush [%w]", err)
	}
	return nil
}
