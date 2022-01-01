package unkodb

import (
	"bytes"
	"io"
	"testing"

	"github.com/neetsdkasu/unkodb/dummyfile"
)

func TestCreateDB(t *testing.T) {
	f := &dummyfile.DummyFile{}
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
	f.Seek(0, io.SeekStart)
	if !bytes.Equal(f.Bytes(), expectBuf) {
		t.Fatal("unmatch buf:", "expect:", expectBuf, "actual", f.Bytes())
	}
}

func TestOpenDB1(t *testing.T) {
	f := &dummyfile.DummyFile{}
	f.Write([]byte{
		0, 0, 0, 0, 0,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, 0, 0, 0, 0,
		0, 32,
		0, 1,
		255, 255, 255, 255,
		255, 255, 255, 255,
		0, 0, 0, 0,
	})
	f.Seek(0, io.SeekStart)

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

	const expectEntriesOffset = expectOffset + 32
	if expectEntriesOffset != db.entriesOffset {
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
	f := &dummyfile.DummyFile{}
	f.Write([]byte{
		1, 2, 3, 4, 5,
		0, 0, 0, 0, 0,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, 0, 0, 0, 0,
		0, 32,
		0, 1,
		255, 255, 255, 255,
		2, 4, 6, 8,
		1, 0, 2, 4,
	})
	f.Seek(5, io.SeekStart)

	db, err := Open(f)
	if err != nil {
		t.Fatal(err)
	}
	if f != db.file {
		t.Fatal("db.file is wrong")
	}
	const expectOffset = 5
	if expectOffset != db.fileOffset {
		t.Fatal("umatch db.fileOffset:",
			"expect:", expectOffset,
			"actual", db.fileOffset,
		)
	}

	const expectEntriesOffset = expectOffset + 32
	if expectEntriesOffset != db.entriesOffset {
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
