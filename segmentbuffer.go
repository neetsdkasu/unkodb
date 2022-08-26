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

// bufferの内容をファイルに書き込む
func (seg *segmentBuffer) Flush() error {
	err := seg.file.Write(seg.position, seg.buffer)
	if err != nil {
		return fmt.Errorf("Failed segmentBuffer.Flush [%w]", err)
	}
	return nil
}

// セグメント内のbufferが一部しかファイルからロードされてない場合に全部のbufferをファイルからロードする
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

// セグメントを２つに分割できるかを確認する
// 分割位置posはseg.Buffer()内の位置
// buffer[:pos]とbuffer[pos:]に分割する
// その際
//
//	 len(buffer[:pos])>minimumSegmentByteSize
//	かつ
//	 len(buffer[pos:])>minimumSegmentTotalByteSize
//
// を満たすべき
func (seg *segmentBuffer) CanSplit(pos int) bool {
	return pos > minimumSegmentByteSize &&
		seg.Size()-pos > minimumSegmentTotalByteSize
}

// セグメントを２つに分割する
// 分割位置posはseg.Buffer()内の位置
// buffer[:pos]とbuffer[pos:]に分割する
func (seg *segmentBuffer) Split(pos int) (*segmentBuffer, error) {
	if !seg.CanSplit(pos) {
		bug.Panic("invalid pos")
	}
	err := seg.LoadFullSegment()
	if err != nil {
		return nil, err
	}
	buf1 := seg.buffer[:pos]
	buf2 := seg.buffer[pos:]
	err = newByteEncoder(newByteSliceWriter(buf1), fileByteOrder).Int32(int32(len(buf1)))
	if err != nil {
		bug.Panic(err) // ここに到達する場合はバグがある
	}
	err = newByteEncoder(newByteSliceWriter(buf2), fileByteOrder).Int32(int32(len(buf2)))
	if err != nil {
		bug.Panic(err) // ここに到達する場合はバグがある
	}
	seg.segmentSize = len(buf1)
	seg.buffer = buf1
	other := &segmentBuffer{
		file:        seg.file,
		position:    seg.position + len(buf1),
		buffer:      buf2,
		segmentSize: len(buf2),
		partial:     false,
	}
	err = other.Flush()
	if err != nil {
		return nil, err
	}
	err = seg.Flush()
	if err != nil {
		return nil, err
	}
	return other, nil
}
