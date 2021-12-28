package unkodb

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

type dummyFile struct {
	Buf    []byte
	Offset int
}

func (f *dummyFile) Read(p []byte) (n int, err error) {
	n = len(p)
	rem := len(f.Buf) - f.Offset
	if rem < n {
		if rem == 0 {
			return 0, io.EOF
		}
		n = rem
	}
	copy(p[:n], f.Buf[f.Offset:f.Offset+n])
	f.Offset += n
	return
}

func (f *dummyFile) Write(p []byte) (n int, err error) {
	n = len(p)
	rem := len(f.Buf) - f.Offset
	if rem < n {
		copy(f.Buf[f.Offset:], p[:rem])
		f.Buf = append(f.Buf, p[rem:]...)
	} else {
		copy(f.Buf[f.Offset:f.Offset+n], p)
	}
	f.Offset += n
	return
}

func (f *dummyFile) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = int64(f.Offset) + offset
	case io.SeekEnd:
		newOffset = int64(len(f.Buf)) + offset
	default:
		return int64(f.Offset), fmt.Errorf("invalid whence %d", whence)
	}
	if newOffset < 0 {
		return int64(f.Offset), fmt.Errorf("invalid offset %d", offset)
	} else if int64(len(f.Buf)) < newOffset {
		return int64(f.Offset), fmt.Errorf("unsupported extending file")
	}
	f.Offset = int(newOffset)
	return newOffset, nil
}

func TestCreateDB(t *testing.T) {
	f := &dummyFile{}
	if _, err := Create(f); err != nil {
		t.Fatal(err)
	}
	expectBuf := []byte{
		0, 0, 0, 0, 0,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, 0, 0, 0, 0,
		0, 32,
		0, 1,
		255, 255, 255, 255,
		255, 255, 255, 255,
		0, 0, 0, 0,
	}
	if !bytes.Equal(f.Buf, expectBuf) {
		t.Fatal("unmatch buf:", "expect:", expectBuf, "actual", f.Buf)
	}
}

func TestOpenDB1(t *testing.T) {
	f := &dummyFile{}
	f.Offset = 0
	f.Buf = []byte{
		0, 0, 0, 0, 0,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, 0, 0, 0, 0,
		0, 32,
		0, 1,
		255, 255, 255, 255,
		255, 255, 255, 255,
		0, 0, 0, 0,
	}
	db, err := Open(f)
	if err != nil {
		t.Fatal(err)
	}
	if f != db.file {
		t.Fatal("db.file is wrong")
	}
	const expectOffset = 0
	if expectOffset != db.fileOffset {
		t.Fatal("umatch db.fileOffset:",
			"expect:", expectOffset,
			"actual", db.fileOffset,
		)
	}
	if expectOffset+headerSize != db.entriesOffset {
		t.Fatal("umatch db.entriesOffset:",
			"expect:", expectOffset+headerSize,
			"actual", db.entriesOffset,
		)
	}

	// TODO 他のパラメータチェック必要ね

	const expectEntriesTotalByteSize = 0
	if expectEntriesTotalByteSize != db.entriesTotalByteSize {
		t.Fatal("umatch db.entriesTotalByteSize:",
			"expect:", expectEntriesTotalByteSize,
			"actual", db.entriesTotalByteSize,
		)
	}
}

func TestOpenDB2(t *testing.T) {
	f := &dummyFile{}
	f.Offset = 0
	f.Buf = []byte{
		0, 0, 0, 0, 0,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, 0, 0, 0, 0,
		0, 32,
		0, 1,
		1, 3, 5, 7,
		2, 4, 6, 8,
		1, 0, 2, 4,
	}
	db, err := Open(f)
	if err != nil {
		t.Fatal(err)
	}
	if f != db.file {
		t.Fatal("db.file is wrong")
	}
	const expectOffset = 0
	if expectOffset != db.fileOffset {
		t.Fatal("umatch db.fileOffset:",
			"expect:", expectOffset,
			"actual", db.fileOffset,
		)
	}
	if expectOffset+headerSize != db.entriesOffset {
		t.Fatal("umatch db.entriesOffset:",
			"expect:", expectOffset+headerSize,
			"actual", db.entriesOffset,
		)
	}

	// TODO 他のパラメータチェック必要ね

	const expectEntriesTotalByteSize = 0x01_00_02_04
	if expectEntriesTotalByteSize != db.entriesTotalByteSize {
		t.Fatal("umatch db.entriesTotalByteSize:",
			"expect:", expectEntriesTotalByteSize,
			"actual", db.entriesTotalByteSize,
		)
	}
}
