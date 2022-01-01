package dummyfile

import (
	"bytes"
	"io"
	"testing"
	"testing/quick"
)

func TestNew_singleBufferSize(t *testing.T) {
	f := func(s uint16) bool {
		ss := int(s)
		file := New(ss)
		if 0 < ss%1024 {
			ss += 1024 - (ss % 1024)
		}
		return len(file.singleBuffer) == ss
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestNew_bufferSize(t *testing.T) {
	f := func(s uint16) bool {
		ss := int(s)
		file := New(ss)
		if 0 < ss%1024 {
			ss += 1024 - (ss % 1024)
		}
		bs := ss / 1024
		return len(file.buffer) == bs
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestNew_sharing(t *testing.T) {
	f := func(s uint16) bool {
		ss := int(s)
		file := New(ss)
		for i := range file.singleBuffer {
			v := 123 + (i & 1023) + (i >> 10)
			file.singleBuffer[i] = byte(v)
		}
		for i, buf := range file.buffer {
			for k, b := range buf {
				v := byte(123 + k + i)
				if b != v {
					return false
				}
				buf[k] = byte(99 + k + i)
			}
		}
		for i, b := range file.singleBuffer {
			v := byte(99 + (i & 1023) + (i >> 10))
			if b != v {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCap(t *testing.T) {
	t.Skip("テスト未実装")
}

func TestLen(t *testing.T) {
	t.Skip("テスト未実装")
}

func TestSeek(t *testing.T) {
	t.Skip("テスト未実装")
}

func TestGrow(t *testing.T) {
	t.Skip("テスト未実装")
}

func TestUnite(t *testing.T) {
	t.Skip("テスト未実装")
}

func TestBytes(t *testing.T) {
	t.Skip("テスト未実装")
}

func TestWrite(t *testing.T) {
	file := new(DummyFile)
	buf := make([]byte, 0, 1700*10)
	bufUpper := make([]byte, 1500)
	bufLower := make([]byte, 200)
	for i := 0; i < 10; i++ {
		for k := range bufUpper {
			bufUpper[k] = byte('A' + i)
		}
		for k := range bufLower {
			bufLower[k] = byte('a' + i)
		}
		buf = append(buf, bufUpper...)
		buf = append(buf, bufLower...)
		file.Write(bufUpper)
		file.Write(bufLower)
	}
	// Bytesを使わずに確認すべき・・・
	if !bytes.Equal(buf, file.Bytes()) {
		t.Fatal("unmatch buffer")
	}
}

func TestRead(t *testing.T) {
	file := new(DummyFile)
	buf := make([]byte, 0, 1700*10)
	bufUpper := make([]byte, 1500)
	bufLower := make([]byte, 200)
	for i := 0; i < 10; i++ {
		for k := range bufUpper {
			bufUpper[k] = byte('A' + i)
		}
		for k := range bufLower {
			bufLower[k] = byte('a' + i)
		}
		buf = append(buf, bufUpper...)
		buf = append(buf, bufLower...)
		file.Write(bufUpper)
		file.Write(bufLower)
	}
	file.Seek(1101*5, io.SeekStart)
	// SeekやWriteを使わずにテストすべき・・・
	readBuf := make([]byte, 1700*11)
	n, _ := file.Read(readBuf)
	readBuf = readBuf[:n]
	if !bytes.Equal(buf[1101*5:], readBuf) {
		t.Fatal("unmatch buffer")
	}
}
