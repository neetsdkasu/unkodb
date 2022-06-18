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
	if fileFormatVersion != 1 {
		t.Fatal("Wrong fileFormatVersion")
	}
	if addressByteSize != 4 {
		t.Fatal("Wrong addressByteSize")
	}
	if nullAddress != 0 {
		t.Fatal("Wrong nullAddress")
	}
	if fileHeaderSignaturePosition != 0 {
		t.Fatal("Wrong fileHeaderSignaturePosition")
	}
	if fileHeaderSignatureLength != 16 {
		t.Fatal("Wrong fileHeaderSignatureLength")
	}
	if fileHeaderFileFormatVersionPosition != 16 {
		t.Fatal("Wrong fileHeaderFileFormatVersionPosition")
	}
	if fileHeaderFileFormatVersionLength != 2 {
		t.Fatal("Wrong fileHeaderFileFormatVersionLength")
	}
	if fileHeaderNextNewSegmentAddressPosition != 18 {
		t.Fatal("Wrong fileHeaderNextNewSegmentAddressPosition")
	}
	if fileHeaderNextNewSegmentAddressLength != 4 {
		t.Fatal("Wrong fileHeaderNextNewSegmentAddressLength")
	}
	if fileHeaderReserveAreaAddressPosition != 22 {
		t.Fatal("Wrong fileHeaderReserveAreaAddressPosition")
	}
	if fileHeaderReserveAreaAddressLength != 4 {
		t.Fatal("Wrong fileHeaderReserveAreaAddressLength")
	}
	if fileHeaderTableListRootAddressPosition != 26 {
		t.Fatal("Wrong fileHeaderTableListRootAddressPosition")
	}
	if fileHeaderTableListRootAddressLength != 4 {
		t.Fatal("Wrong fileHeaderTableListRootAddressLength")
	}
	if fileHeaderIdleSegmentTreeRootAddressPosition != 30 {
		t.Fatal("Wrong fileHeaderIdleSegmentTreeRootAddressPosition")
	}
	if fileHeaderIdleSegmentTreeRootAddressLength != 4 {
		t.Fatal("Wrong fileHeaderIdleSegmentTreeRootAddressLength")
	}
	if fileHeaderByteSize != 34 {
		t.Fatal("Wrong fileHeaderByteSize")
	}
	if firstNewSegmentAddress != 34 {
		t.Fatal("Wrong firstNewSegmentAddress")
	}
	if segmentHeaderByteSize != 4 {
		t.Fatal("Wrong segmentHeaderByteSize")
	}
}

func TestFileSignature(t *testing.T) {
	sig := fileSignature()
	if len(sig) != fileHeaderSignatureLength {
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
	comp2 := bytes.Equal(fileSignature(), []byte{
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

	file, err := initializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}
	if file.version != fileFormatVersion {
		t.Fatalf("Wrong File Format Version (%d)", file.version)
	}
	if file.nextNewSegmentAddress != firstNewSegmentAddress {
		t.Fatalf("Wrong NextNewSegmentAddress (%d)", file.nextNewSegmentAddress)
	}
	if file.tableListRootAddress != nullAddress {
		t.Fatalf("Wrong TableListRootAddress (%d)", file.tableListRootAddress)
	}
	if file.idleSegmentListRootAddress != nullAddress {
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

	if len(buf) != fileHeaderByteSize {
		t.Fatalf("Wrong File Size (%d)", len(buf))
	}

	comp := bytes.Equal(buf, []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, fileFormatVersion,
		0, 0, 0, firstNewSegmentAddress,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
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

	_, err = initializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	file, err := readFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}
	if file.version != fileFormatVersion {
		t.Fatalf("Wrong File Format Version (%d)", file.version)
	}
	if file.nextNewSegmentAddress != firstNewSegmentAddress {
		t.Fatalf("Wrong NextNewSegmentAddress (%d)", file.nextNewSegmentAddress)
	}
	if file.tableListRootAddress != nullAddress {
		t.Fatalf("Wrong TableListRootAddress (%d)", file.tableListRootAddress)
	}
	if file.idleSegmentListRootAddress != nullAddress {
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

	if len(buf) != fileHeaderByteSize {
		t.Fatalf("Wrong File Size (%d)", len(buf))
	}

	comp := bytes.Equal(buf, []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, fileFormatVersion,
		0, 0, 0, firstNewSegmentAddress,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestFileAccessor_UpdateNextNewSegmentAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
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

	if len(buf) != fileHeaderByteSize {
		t.Fatalf("Wrong File Size (%d)", len(buf))
	}

	comp := bytes.Equal(buf, []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, fileFormatVersion,
		(Address >> 24) & 0xFF, (Address >> 16) & 0xFF, (Address >> 8) & 0xFF, Address & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestFileAccessor_NextNewSegmentAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	nextNewSegmentAddress := file.NextNewSegmentAddress()
	if nextNewSegmentAddress != firstNewSegmentAddress {
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

func TestFileAccessor_UpdateTableListRootAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
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

	if len(buf) != fileHeaderByteSize {
		t.Fatalf("Wrong File Size (%d)", len(buf))
	}

	comp := bytes.Equal(buf, []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, fileFormatVersion,
		0, 0, 0, firstNewSegmentAddress,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(Address >> 24) & 0xFF, (Address >> 16) & 0xFF, (Address >> 8) & 0xFF, Address & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestFileAccessor_TableListRootAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	tableListRootAddress := file.TableListRootAddress()
	if tableListRootAddress != nullAddress {
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

func TestFileAccessor_UpdateIdleSegmentTreeRootAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
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

	if len(buf) != fileHeaderByteSize {
		t.Fatalf("Wrong File Size (%d)", len(buf))
	}

	comp := bytes.Equal(buf, []byte{
		3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
		'U', 'N', 'K', 'O', 'D', 'B',
		0, fileFormatVersion,
		0, 0, 0, firstNewSegmentAddress,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
		(Address >> 24) & 0xFF, (Address >> 16) & 0xFF, (Address >> 8) & 0xFF, Address & 0xFF,
	})

	if !comp {
		t.Fatalf("Wrong File Format (%#v)", buf)
	}
}

func TestFileAccessor_IdleSegmentTreeRootAddress(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
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

func TestFileAccessor_CreateSegment(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const DataSize1 = 10
	const SegmentSize1 = DataSize1 + segmentHeaderByteSize
	{
		seg1, err := file.CreateSegment(DataSize1)
		if err != nil {
			t.Fatal(err)
		}

		if seg1.file != file {
			t.Fatal("Wrong Segment.file pointer")
		}

		if seg1.position != fileHeaderByteSize {
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

		if len(buf) != fileHeaderByteSize+SegmentSize1 {
			t.Fatalf("Wrong File Size (%d)", len(buf))
		}

		comp := bytes.Equal(buf, []byte{
			3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
			'U', 'N', 'K', 'O', 'D', 'B',
			0, fileFormatVersion,
			0, 0, 0, firstNewSegmentAddress + SegmentSize1,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
			0, 0, 0, SegmentSize1,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		})

		if !comp {
			t.Fatalf("Wrong File Writing (%#v)", buf)
		}
	}

	const DataSize2 = 7
	const SegmentSize2 = DataSize2 + segmentHeaderByteSize
	{
		seg2, err := file.CreateSegment(DataSize2)
		if err != nil {
			t.Fatal(err)
		}

		if seg2.file != file {
			t.Fatal("Wrong Segment.file pointer")
		}

		if seg2.position != fileHeaderByteSize+SegmentSize1 {
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

		if len(buf) != fileHeaderByteSize+SegmentSize1+SegmentSize2 {
			t.Fatalf("Wrong File Size (%d)", len(buf))
		}

		comp := bytes.Equal(buf, []byte{
			3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
			'U', 'N', 'K', 'O', 'D', 'B',
			0, fileFormatVersion,
			0, 0, 0, firstNewSegmentAddress + SegmentSize1 + SegmentSize2,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
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

func TestSegmentBuffer_Position(t *testing.T) {
	f := func(v int) bool {
		seg := &segmentBuffer{}
		seg.position = v
		position := seg.Position()
		return position == v
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
	g := func(v int) bool {
		seg := &segmentBuffer{}
		seg.position = v
		position := seg.Position()
		return seg.position == v && position == v
	}
	if err := quick.Check(g, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentBuffer_Buffer(t *testing.T) {
	seg := &segmentBuffer{}
	seg.buffer = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

	buf := seg.Buffer()

	if len(buf)+segmentHeaderByteSize != len(seg.buffer) {
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

func TestSegmentBuffer_Flush(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const DataSize1 = 12
	const SegmentSize1 = DataSize1 + segmentHeaderByteSize
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

		if len(buf) != fileHeaderByteSize+SegmentSize1 {
			t.Fatalf("Wrong File Size (%d)", len(buf))
		}

		comp := bytes.Equal(buf, []byte{
			3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
			'U', 'N', 'K', 'O', 'D', 'B',
			0, fileFormatVersion,
			0, 0, 0, firstNewSegmentAddress + SegmentSize1,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
			(nullAddress >> 24) & 0xFF, (nullAddress >> 16) & 0xFF, (nullAddress >> 8) & 0xFF, nullAddress & 0xFF,
			0, 0, 0, SegmentSize1,
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
		})

		if !comp {
			t.Fatalf("Wrong File Writing (%#v)", buf)
		}
	}
}

func TestFileAccessor_ReadSegment(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := initializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	const DataSize1 = 8
	const SegmentSize1 = DataSize1 + segmentHeaderByteSize
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
	const SegmentSize2 = DataSize2 + segmentHeaderByteSize
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
