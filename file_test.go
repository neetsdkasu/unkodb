package unkodb

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// え、定数のテストっているの？
func TestFileHeaderConstantValues(t *testing.T) {
	if AddressSize != 4 {
		t.Fatal("Wrong AddressSize")
	}
	if FileHeaderSignaturePosition != 0 {
		t.Fatal("Wrong FileHeaderSignaturePosition")
	}
	if FileHeaderSignatureLength != 16 {
		t.Fatal("Wrong FileHeaderSignatureLength")
	}
	if FileHeaderFileFormatVersionPosition != 16 {
		t.Fatal("Wrong FileHeaderFileFormatVersionPosition")
	}
	if FileHeaderFileFormatVersionLength != 2 {
		t.Fatal("Wrong FileHeaderFileFormatVersionLength")
	}
	if FileHeaderReserveAreaAddressPosition != 18 {
		t.Fatal("Wrong FileHeaderReserveAreaAddressPosition")
	}
	if FileHeaderReserveAreaAddressLength != 4 {
		t.Fatal("Wrong FileHeaderReserveAreaAddressLength")
	}
	if FileHeaderTableListRootAddressPosition != 22 {
		t.Fatal("Wrong FileHeaderTableListRootAddressPosition")
	}
	if FileHeaderTableListRootAddressLength != 4 {
		t.Fatal("Wrong FileHeaderTableListRootAddressLength")
	}
	if FileHeaderIdleSegmentListRootAddressPosition != 26 {
		t.Fatal("Wrong FileHeaderIdleSegmentListRootAddressPosition")
	}
	if FileHeaderIdleSegmentListRootAddressLength != 4 {
		t.Fatal("Wrong FileHeaderIdleSegmentListRootAddressLength")
	}
	if FileHeaderSize != 30 {
		t.Fatal("Wrong FileHeaderSize")
	}
	if SegmentHeaderSize != 4 {
		t.Fatal("Wrong SegmentHeaderSize")
	}
}

func TestSignature(t *testing.T) {
	sig := Signature()
	if len(sig) != FileHeaderSignatureLength {
		t.Fatal("Wrong Signature Length")
	}
	comp := bytes.Equal(sig, []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
	})
	if !comp {
		t.Fatal("Wrong Signature")
	}
	for i := range sig {
		sig[i]++
	}
	comp2 := bytes.Equal(Signature(), []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
	})
	if !comp2 {
		t.Fatal("Wrong Signature (2)")
	}
}

func TestNewFile(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := NewFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}
	if file.version != FileFormatVersion {
		t.Fatalf("Wrong File Format Version (%d)", file.version)
	}
	if file.fileSize != FileHeaderSize {
		t.Fatalf("Wrong File Size (%d)", file.fileSize)
	}

	file, err = NewFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}
	if file.version != FileFormatVersion {
		t.Fatalf("Wrong File Format Version (%d)", file.version)
	}
	if file.fileSize != FileHeaderSize {
		t.Fatalf("Wrong FileS ize (%d)", file.fileSize)
	}

	_, err = tempfile.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	buf, err := ioutil.ReadAll(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	if len(buf) != FileHeaderSize {
		t.Fatalf("Wrong File Size (%d)", len(buf))
	}

	comp := bytes.Equal(buf, []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, 1,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}
