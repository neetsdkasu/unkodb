// Author:  Leonardone @ NEETSDKASU
// License: MIT

// io.ReadWriteSeeker を実装した構造体　DummyFile を定義する
// DummyFile は github.com/neetsdkasu/unkodb の内部のテスト用に使う (それ以外の使われ方は想定されていない)
// バッファを1024byte単位の断片としてデータを保持する
// バッファ内の現在の位置の情報はRead,Writeで共通なためいずれかの実行で更新される
// bytes.Bufferやos.Fileのメソッドとは挙動が異なるので注意
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

// 初期容量を指定してDummyFileのインスタンスを生成する
// initialwCapacityは1024の倍数になるように切り上げされる
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

// 現在の容量を返す
func (f *DummyFile) Cap() int {
	return len(f.buffer) << bitWidth
}

// 現在位置から読み込み可能な残りサイズを返す
// Readで使用することが想定されている
// 現在の位置はWrite,Read,Seekで変わる
func (f *DummyFile) Len() int {
	return f.length - f.position
}

// 現在の位置からデータを読み込む
// 現在の位置は読み込んだ長さ分だけ進む
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

// 現在の位置からデータを書き込む
// 必要に応じて容量が増加される
// 現在の位置は書き込んだ長さ分だけ進む
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

// 現在の位置を変更する
// 現在の容量より大きい位置が指定された場合はGrowの呼び出しで容量が拡張される
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

// 必要に応じて容量を増やす
// newCapacityは1024の倍数になるように切り上げされる
// 現在の容量がnewCapacity未満の場合は容量が増やされる
// 現在の容量がnewCapacity居ｋ上の場合は何もしない
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

// 内部で保持してる全てのバッファ断片を結合して1つのバッファにする
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

// 現在の位置から残り全部までのバッファのスライスを返す
// 1つのバッファ断片内に収まる範囲の場合はその断片からスライスを返す
// 1つのバッファ断片内に収まらない範囲の場合はUniteを呼び出し全ての断片を結合してからスライスを返す
// f.Len() == len(f.Bytes()) となる
// 現在の位置はWrite,Read,Seekで変わる
func (f *DummyFile) Bytes() []byte {
	a := f.position >> bitWidth
	b := f.position & bitMask
	if f.Len() < cap(f.buffer[a])-b {
		return f.buffer[a][b : b+f.Len()]
	} else {
		f.Unite()
		return f.singleBuffer[f.position:f.length]
	}
}
