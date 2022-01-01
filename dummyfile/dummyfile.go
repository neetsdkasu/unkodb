package dummyfile

import (
	"fmt"
	"io"
	"math"
)

const (
	bitWidth  = 10
	blockSize = 1 << bitWidth
	bitMask   = blockSize - 1
)

type DummyFile struct {
	buffer       [][]byte
	singleBuffer []byte
	position     int
	length       int
}

func New(initialCapacity int) *DummyFile {
	if initialCapacity < 0 {
		panic("initialCapacity must be non-negative value")
	}
	if initialCapacity+bitMask <= 0 {
		panic("exceed limit capacity")
	}
	bufferCount := (initialCapacity + bitMask) >> bitWidth
	capacity := bufferCount << bitWidth
	singleBuffer := make([]byte, capacity)
	buffer := make([][]byte, bufferCount)
	for i := range buffer {
		p := i << bitWidth
		buffer[i] = singleBuffer[p : p+blockSize]
	}
	return &DummyFile{
		buffer:       buffer,
		singleBuffer: singleBuffer,
		position:     0,
		length:       0,
	}
}

func (f *DummyFile) Read(p []byte) (n int, err error) {
	n = f.length - f.position
	if n == 0 {
		err = io.EOF
		return
	}
	if len(p) < n {
		n = len(p)
	}
	if f.singleBuffer != nil {
		copy(p[:n], f.singleBuffer[f.position:f.position+n])
		f.position += n
		return
	}
	a := f.position >> bitWidth
	b := f.position & bitMask
	s := blockSize - b
	if n < s {
		s = n
	}
	copy(p[:s], f.buffer[a][b:b+s])
	rem := n - s
	p = p[s:]
	s = blockSize
	for 0 < rem {
		a++
		if rem < blockSize {
			s = rem
		}
		copy(p[:s], f.buffer[a][:s])
		p = p[s:]
		rem -= s
	}
	f.position += n
	return
}

func (f *DummyFile) Write(p []byte) (n int, err error) {
	n = len(p)
	newPosition := f.position + n
	if newPosition <= 0 {
		n = 0
		err = fmt.Errorf("exceed limit capacity")
		return
	}
	f.Grow(newPosition)
	if f.singleBuffer != nil {
		copy(f.singleBuffer[f.position:newPosition], p)
		if f.length < newPosition {
			f.length = newPosition
		}
		f.position = newPosition
		return
	}
	a := f.position >> bitWidth
	b := f.position & bitMask
	s := blockSize - b
	if n < s {
		s = n
	}
	copy(f.buffer[a][b:b+s], p[:s])
	rem := n - s
	p = p[s:]
	s = blockSize
	for 0 < rem {
		a++
		if rem < blockSize {
			s = rem
		}
		copy(f.buffer[a][:s], p[:s])
		p = p[s:]
		rem -= s
	}
	if f.length < newPosition {
		f.length = newPosition
	}
	f.position = newPosition
	return
}

func (f *DummyFile) Seek(offset int64, whence int) (int64, error) {
	var newPosition int64
	switch whence {
	case io.SeekStart:
		newPosition = offset
	case io.SeekCurrent:
		newPosition = int64(f.position) + offset
		// オーバーフローチェック、どうすれば・・・？
		if 0 < offset && newPosition <= int64(f.position) {
			// 0 < offset のとき オーバーフローしていなければ f.position < newPosition を満たすはず
			return int64(f.position), fmt.Errorf("invalid offset (%d)", offset)
		} else if offset < 0 && int64(f.position) <= newPosition {
			// offset < 0 のとき オーバーフローしていなければ newPosition < f.position を満たすはず
			return int64(f.position), fmt.Errorf("invalid offset (%d)", offset)
		}
	case io.SeekEnd:
		newPosition = int64(f.length) + offset
		// オーバーフローチェック、どうすれば・・・？
		if 0 < offset && newPosition <= int64(f.length) {
			// 0 < offset のとき オーバーフローしていなければ f.length < newPosition を満たすはず
			return int64(f.position), fmt.Errorf("invalid offset (%d)", offset)
		} else if offset < 0 && int64(f.length) <= newPosition {
			// offset < 0 のとき オーバーフローしていなければ newPosition < f.length を満たすはず
			return int64(f.position), fmt.Errorf("invalid offset (%d)", offset)
		}
	default:
		return int64(f.position), fmt.Errorf("invalid whence (%d)", whence)
	}
	if newPosition < 0 || math.MaxInt < newPosition {
		return int64(f.position), fmt.Errorf("invalid offset (%d)", offset)
	}
	f.Grow(int(newPosition))
	f.position = int(newPosition)
	return newPosition, nil
}

func (f *DummyFile) Grow(newCapacity int) {
	if newCapacity < 0 {
		panic("invalid newCapacity")
	}
	currentCapacity := len(f.buffer) << bitWidth
	if newCapacity <= currentCapacity {
		return
	}
	if newCapacity+bitMask <= 0 {
		panic("exceed limit capacity")
	}
	req := newCapacity - currentCapacity
	bufferCount := (req + bitMask) >> bitWidth
	reqSize := bufferCount << bitWidth
	tempBuffer := make([]byte, reqSize)
	if len(f.buffer) == 0 {
		f.singleBuffer = tempBuffer
	} else {
		f.singleBuffer = nil
	}
	for 0 < len(tempBuffer) {
		f.buffer = append(f.buffer, tempBuffer[:blockSize])
		tempBuffer = tempBuffer[blockSize:]
	}
}

func (f *DummyFile) Unite() {
	if f.singleBuffer != nil {
		return
	}
	newSize := len(f.buffer) << bitWidth
	newBuffer := make([]byte, newSize)
	f.singleBuffer = newBuffer
	p := 0
	for p < newSize { // コピーは f.length まででもいいのかもしれないが
		b := f.buffer[p>>bitWidth]
		c := cap(b)
		copy(newBuffer[p:p+c], b[:c])
		p += c
	}
	for i := range f.buffer {
		f.buffer[i] = newBuffer[:blockSize]
		newBuffer = newBuffer[blockSize:]
	}
}

func (f *DummyFile) Bytes() []byte {
	f.Unite()
	return f.singleBuffer[:f.length]
}
