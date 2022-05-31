package unkodb

import (
	"bytes"
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
