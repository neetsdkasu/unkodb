// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"testing/quick"
)

// え、定数のテストっているの？
func TestFileHeaderConstantValues(t *testing.T) {
	if FileFormatVersion != 1 {
		t.Fatal("Wrong FileFormatVersion")
	}
	if AddressSize != 4 {
		t.Fatal("Wrong AddressSize")
	}
	if NullAddress != 0 {
		t.Fatal("Wrong NullAddress")
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
	if FileHeaderNextNewSegmentAddressPosition != 18 {
		t.Fatal("Wrong FileHeaderNextNewSegmentAddressPosition")
	}
	if FileHeaderNextNewSegmentAddressLength != 4 {
		t.Fatal("Wrong FileHeaderNextNewSegmentAddressLength")
	}
	if FileHeaderReserveAreaAddressPosition != 22 {
		t.Fatal("Wrong FileHeaderReserveAreaAddressPosition")
	}
	if FileHeaderReserveAreaAddressLength != 4 {
		t.Fatal("Wrong FileHeaderReserveAreaAddressLength")
	}
	if FileHeaderTableListRootAddressPosition != 26 {
		t.Fatal("Wrong FileHeaderTableListRootAddressPosition")
	}
	if FileHeaderTableListRootAddressLength != 4 {
		t.Fatal("Wrong FileHeaderTableListRootAddressLength")
	}
	if FileHeaderIdleSegmentTreeRootAddressPosition != 30 {
		t.Fatal("Wrong FileHeaderIdleSegmentTreeRootAddressPosition")
	}
	if FileHeaderIdleSegmentTreeRootAddressLength != 4 {
		t.Fatal("Wrong FileHeaderIdleSegmentTreeRootAddressLength")
	}
	if FileHeaderSize != 34 {
		t.Fatal("Wrong FileHeaderSize")
	}
	if FirstNewSegmentAddress != 34 {
		t.Fatal("Wrong FirstNewSegmentAddress")
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

func TestInitializeFile(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}
	if file.version != FileFormatVersion {
		t.Fatalf("Wrong File Format Version (%d)", file.version)
	}
	if file.nextNewSegmentAddress != FirstNewSegmentAddress {
		t.Fatalf("Wrong NextNewSegmentAddress (%d)", file.nextNewSegmentAddress)
	}
	if file.tableListRootAddress != NullAddress {
		t.Fatalf("Wrong TableListRootAddress (%d)", file.tableListRootAddress)
	}
	if file.idleSegmentListRootAddress != NullAddress {
		t.Fatalf("Wrong IdleSegmentTreeRootAddress (%d)", file.idleSegmentListRootAddress)
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
		0, FileFormatVersion,
		0, 0, 0, FirstNewSegmentAddress,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestReadFile(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	_, err = InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	file, err := ReadFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}
	if file.version != FileFormatVersion {
		t.Fatalf("Wrong File Format Version (%d)", file.version)
	}
	if file.nextNewSegmentAddress != FirstNewSegmentAddress {
		t.Fatalf("Wrong NextNewSegmentAddress (%d)", file.nextNewSegmentAddress)
	}
	if file.tableListRootAddress != NullAddress {
		t.Fatalf("Wrong TableListRootAddress (%d)", file.tableListRootAddress)
	}
	if file.idleSegmentListRootAddress != NullAddress {
		t.Fatalf("Wrong IdleSegmentTreeRootAddress (%d)", file.idleSegmentListRootAddress)
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
		0, FileFormatVersion,
		0, 0, 0, FirstNewSegmentAddress,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestFile_UpdateNextNewSegmentAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const Address = 0x12345678

	err = file.UpdateNextNewSegmentAddress(Address)
	if err != nil {
		t.Fatal(err)
	}

	if file.nextNewSegmentAddress != Address {
		t.Fatalf("Wrong NextNewSegmentAddress (%d)", file.nextNewSegmentAddress)
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
		0, FileFormatVersion,
		(Address >> 24) & 0xFF, (Address >> 16) & 0xFF, (Address >> 8) & 0xFF, Address & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestFile_NextNewSegmentAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	nextNewSegmentAddress := file.NextNewSegmentAddress()
	if nextNewSegmentAddress != FirstNewSegmentAddress {
		t.Fatalf("Wrong NextNewSegmentAddress (%d)", nextNewSegmentAddress)
	}

	const Address = 0x12345678

	err = file.UpdateNextNewSegmentAddress(Address)
	if err != nil {
		t.Fatal(err)
	}

	nextNewSegmentAddress = file.NextNewSegmentAddress()
	if nextNewSegmentAddress != Address {
		t.Fatalf("Wrong NextNewSegmentAddress (%d)", nextNewSegmentAddress)
	}
}

func TestFile_UpdateTableListRootAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const Address = 0x12345678

	err = file.UpdateTableListRootAddress(Address)
	if err != nil {
		t.Fatal(err)
	}

	if file.tableListRootAddress != Address {
		t.Fatalf("Wrong TableListRootAddress (%d)", file.tableListRootAddress)
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
		0, FileFormatVersion,
		0, 0, 0, FirstNewSegmentAddress,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(Address >> 24) & 0xFF, (Address >> 16) & 0xFF, (Address >> 8) & 0xFF, Address & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestFile_TableListRootAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	tableListRootAddress := file.TableListRootAddress()
	if tableListRootAddress != 0 {
		t.Fatalf("Wrong TableListRootAddress (%d)", tableListRootAddress)
	}

	const Address = 0x12345678

	err = file.UpdateTableListRootAddress(Address)
	if err != nil {
		t.Fatal(err)
	}

	tableListRootAddress = file.TableListRootAddress()
	if tableListRootAddress != Address {
		t.Fatalf("Wrong TableListRootAddress (%d)", tableListRootAddress)
	}
}

func TestFile_UpdateIdleSegmentTreeRootAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const Address = 0x12345678

	err = file.UpdateIdleSegmentTreeRootAddress(Address)
	if err != nil {
		t.Fatal(err)
	}

	if file.idleSegmentListRootAddress != Address {
		t.Fatalf("Wrong IdleSegmentTreeRootAddress (%d)", file.idleSegmentListRootAddress)
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
		0, FileFormatVersion,
		0, 0, 0, FirstNewSegmentAddress,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
		(Address >> 24) & 0xFF, (Address >> 16) & 0xFF, (Address >> 8) & 0xFF, Address & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestFile_IdleSegmentTreeRootAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	idleSegmentListRootAddress := file.IdleSegmentTreeRootAddress()
	if idleSegmentListRootAddress != 0 {
		t.Fatalf("Wrong IdleSegmentTreeRootAddress (%d)", idleSegmentListRootAddress)
	}

	const Address = 0x12345678

	err = file.UpdateIdleSegmentTreeRootAddress(Address)
	if err != nil {
		t.Fatal(err)
	}

	idleSegmentListRootAddress = file.IdleSegmentTreeRootAddress()
	if idleSegmentListRootAddress != Address {
		t.Fatalf("Wrong IdleSegmentTreeRootAddress (%d)", idleSegmentListRootAddress)
	}
}

func TestFile_CreateSegment(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const DataSize1 = 10
	const SegmentSize1 = DataSize1 + SegmentHeaderSize
	{
		seg1, err := file.CreateSegment(DataSize1)
		if err != nil {
			t.Fatal(err)
		}

		if seg1.file != file {
			t.Fatal("Wrong Segment.file pointer")
		}

		if seg1.position != FileHeaderSize {
			t.Fatalf("Wrong Segment.position (%d)", seg1.position)
		}

		comp := bytes.Equal(seg1.buffer, []byte{
			0, 0, 0, SegmentSize1,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		})

		if !comp {
			t.Fatalf("Wrong Segment.buffer (%#v)", seg1.buffer)
		}
	}

	{
		_, err = tempfile.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}

		buf, err := ioutil.ReadAll(tempfile)
		if err != nil {
			t.Fatal(err)
		}

		if len(buf) != FileHeaderSize+SegmentSize1 {
			t.Fatalf("Wrong File Size (%d)", len(buf))
		}

		comp := bytes.Equal(buf, []byte{
			3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
			'U', 'N', 'K', 'O', 'D', 'B',
			0, FileFormatVersion,
			0, 0, 0, FirstNewSegmentAddress + SegmentSize1,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			0, 0, 0, SegmentSize1,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		})

		if !comp {
			t.Fatalf("Wrong File Writing (%#v)", buf)
		}
	}

	const DataSize2 = 7
	const SegmentSize2 = DataSize2 + SegmentHeaderSize
	{
		seg2, err := file.CreateSegment(DataSize2)
		if err != nil {
			t.Fatal(err)
		}

		if seg2.file != file {
			t.Fatal("Wrong Segment.file pointer")
		}

		if seg2.position != FileHeaderSize+SegmentSize1 {
			t.Fatalf("Wrong Segment.position (%d)", seg2.position)
		}

		comp := bytes.Equal(seg2.buffer, []byte{
			0, 0, 0, SegmentSize2,
			0, 0, 0, 0, 0, 0, 0,
		})

		if !comp {
			t.Fatalf("Wrong Segment.buffer (%#v)", seg2.buffer)
		}
	}

	{
		_, err = tempfile.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}

		buf, err := ioutil.ReadAll(tempfile)
		if err != nil {
			t.Fatal(err)
		}

		if len(buf) != FileHeaderSize+SegmentSize1+SegmentSize2 {
			t.Fatalf("Wrong File Size (%d)", len(buf))
		}

		comp := bytes.Equal(buf, []byte{
			3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
			'U', 'N', 'K', 'O', 'D', 'B',
			0, FileFormatVersion,
			0, 0, 0, FirstNewSegmentAddress + SegmentSize1 + SegmentSize2,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			0, 0, 0, SegmentSize1,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, SegmentSize2,
			0, 0, 0, 0, 0, 0, 0,
		})

		if !comp {
			t.Fatalf("Wrong File Writing (%#v)", buf)
		}
	}
}

func TestSegment_Position(t *testing.T) {
	f := func(v int) bool {
		seg := &Segment{}
		seg.position = v
		position := seg.Position()
		return position == v
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
	g := func(v int) bool {
		seg := &Segment{}
		seg.position = v
		position := seg.Position()
		return seg.position == v && position == v
	}
	if err := quick.Check(g, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSegment_Buffer(t *testing.T) {
	seg := &Segment{}
	seg.buffer = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

	buf := seg.Buffer()

	if len(buf)+SegmentHeaderSize != len(seg.buffer) {
		t.Fatalf("Wrong Buffer Size (%d)", len(buf))
	}

	comp := bytes.Equal(buf, []byte{
		5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
	})

	if !comp {
		t.Fatalf("Wrong Buffer (%#v)", buf)
	}

	for i := range buf {
		buf[i] += 100
	}

	comp2 := bytes.Equal(seg.buffer, []byte{
		1, 2, 3, 4,
		105, 106, 107, 108, 109, 110, 111, 112, 113, 114,
	})

	if !comp2 {
		t.Fatalf("Wrong Segment.buffer (%#v)", seg.buffer)
	}
}

func TestSegment_Flush(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const DataSize1 = 12
	const SegmentSize1 = DataSize1 + SegmentHeaderSize
	{
		seg1, err := file.CreateSegment(DataSize1)
		if err != nil {
			t.Fatal(err)
		}

		buf1 := seg1.Buffer()

		for i := range buf1 {
			buf1[i] = byte(i + 1)
		}

		err = seg1.Flush()
		if err != nil {
			t.Fatal(err)
		}
	}

	{
		_, err = tempfile.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}

		buf, err := ioutil.ReadAll(tempfile)
		if err != nil {
			t.Fatal(err)
		}

		if len(buf) != FileHeaderSize+SegmentSize1 {
			t.Fatalf("Wrong File Size (%d)", len(buf))
		}

		comp := bytes.Equal(buf, []byte{
			3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
			'U', 'N', 'K', 'O', 'D', 'B',
			0, FileFormatVersion,
			0, 0, 0, FirstNewSegmentAddress + SegmentSize1,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			(NullAddress >> 24) & 0xFF, (NullAddress >> 16) & 0xFF, (NullAddress >> 8) & 0xFF, NullAddress & 0xFF,
			0, 0, 0, SegmentSize1,
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
		})

		if !comp {
			t.Fatalf("Wrong File Writing (%#v)", buf)
		}
	}
}

func TestFile_ReadSegment(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const DataSize1 = 8
	const SegmentSize1 = DataSize1 + SegmentHeaderSize
	var pos1 int
	{
		seg1, err := file.CreateSegment(DataSize1)
		if err != nil {
			t.Fatal(err)
		}
		pos1 = seg1.Position()

		buf1 := seg1.Buffer()

		for i := range buf1 {
			buf1[i] = byte(i + 1)
		}

		err = seg1.Flush()
		if err != nil {
			t.Fatal(err)
		}
	}

	const DataSize2 = 11
	const SegmentSize2 = DataSize2 + SegmentHeaderSize
	var pos2 int
	{
		seg2, err := file.CreateSegment(DataSize2)
		if err != nil {
			t.Fatal(err)
		}
		pos2 = seg2.Position()

		buf2 := seg2.Buffer()

		for i := range buf2 {
			buf2[i] = byte(100 - i)
		}

		err = seg2.Flush()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 2; i++ {
		{
			seg1, err := file.ReadSegment(pos1)
			if err != nil {
				t.Fatal(err)
			}

			if seg1.file != file {
				t.Fatalf("Wrong Segment.file pointer [%d]", i)
			}

			if seg1.position != pos1 {
				t.Fatalf("Wrong Segment.position (%d) [%d]", seg1.position, i)
			}

			if len(seg1.buffer) != SegmentSize1 {
				t.Fatalf("Wrong Segment.buffer length(%d) [%d]", len(seg1.buffer), i)
			}

			comp := bytes.Equal(seg1.buffer, []byte{
				0, 0, 0, SegmentSize1,
				1, 2, 3, 4, 5, 6, 7, 8,
			})

			if !comp {
				t.Fatalf("Wrong Segment.buffer (%#v) [%d]", seg1.buffer, i)
			}
		}

		{
			seg2, err := file.ReadSegment(pos2)
			if err != nil {
				t.Fatal(err)
			}

			if seg2.file != file {
				t.Fatalf("Wrong Segment.file pointer [%d]", i)
			}

			if seg2.position != pos2 {
				t.Fatalf("Wrong Segment.position (%d) [%d]", seg2.position, i)
			}

			if len(seg2.buffer) != SegmentSize2 {
				t.Fatalf("Wrong Segment.buffer length(%d) [%d]", len(seg2.buffer), i)
			}

			comp := bytes.Equal(seg2.buffer, []byte{
				0, 0, 0, SegmentSize2,
				100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90,
			})

			if !comp {
				t.Fatalf("Wrong Segment.buffer (%#v) [%d]", seg2.buffer, i)
			}
		}
	}
}
