package dummyfile

import (
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
