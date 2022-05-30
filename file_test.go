package unkodb

import (
	"bytes"
	"testing"
)

// え、定数のテストっているの？
func TestFileHeaderConstantValues(t *testing.T) {
	if FileHeaderSignaturePosition != 0 {
		t.Fatal("Wrong FileHeaderSignaturePosition")
	}
	if FileHeaderSignatureLength != 16 {
		t.Fatal("Wrong FileHeaderSignatureLength")
	}
	if FileHeaderFileFormatVersionPosition != 16 {
		t.Fatal("Wrong FileHeaderFileFormatVersionPosition")
	}
	// TODO: check others
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
}
