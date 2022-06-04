// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"
	"io"
	"testing"
)

func TestByteSliceWriter(t *testing.T) {
	buf1 := make([]byte, 10)

	w1 := NewByteSliceWriter(buf1[3:8])

	n1, err := w1.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})

	if n1 != 5 {
		t.Fatalf("Wrong length (%d)", n1)
	}

	if err != io.ErrShortWrite {
		t.Fatalf("Wrong error (%#v)", err)
	}

	comp1 := bytes.Equal(buf1, []byte{
		0, 0, 0, 1, 2, 3, 4, 5, 0, 0,
	})

	if !comp1 {
		t.Fatalf("Wrong writing (%#v)", buf1)
	}
}

func TestByteEncoder(t *testing.T) {
	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

func TestByteDecoder(t *testing.T) {
	t.Skip("TEST IS NOT IMPLEMENTED YET")
}
